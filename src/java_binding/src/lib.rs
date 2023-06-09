// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(lazy_cell)]
#![feature(type_alias_impl_trait)]

mod hummock_iterator;
mod stream_chunk_iterator;

use std::backtrace::Backtrace;
use std::marker::PhantomData;
use std::ops::Deref;
use std::slice::from_raw_parts;
use std::sync::{Arc, LazyLock};

use hummock_iterator::{HummockJavaBindingIterator, KeyedRow};
use jni::objects::{
    AutoArray, GlobalRef, JClass, JMethodID, JObject, JStaticMethodID, JString, JValue, ReleaseMode,
};
use jni::signature::ReturnType;
use jni::sys::{
    jboolean, jbyte, jbyteArray, jdouble, jfloat, jint, jlong, jobject, jshort, jsize, jvalue,
};
use jni::JNIEnv;
use once_cell::sync::OnceCell;
use prost::{DecodeError, Message};
use risingwave_common::array::{ArrayError, StreamChunk};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::ScalarRefImpl;
use risingwave_common::util::panic::rw_catch_unwind;
use risingwave_storage::error::StorageError;
use thiserror::Error;
use tokio::runtime::Runtime;

use crate::stream_chunk_iterator::{StreamChunkIterator, StreamChunkRow};

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| tokio::runtime::Runtime::new().unwrap());

#[derive(Error, Debug)]
enum BindingError {
    #[error("JniError {error}")]
    Jni {
        #[from]
        error: jni::errors::Error,
        backtrace: Backtrace,
    },

    #[error("StorageError {error}")]
    Storage {
        #[from]
        error: StorageError,
        backtrace: Backtrace,
    },

    #[error("DecodeError {error}")]
    Decode {
        #[from]
        error: DecodeError,
        backtrace: Backtrace,
    },

    #[error("StreamChunkArrayError {error}")]
    StreamChunkArray {
        #[from]
        error: ArrayError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, BindingError>;

/// Wrapper around [`jbyteArray`] that adds a lifetime and provides utilities to manipulate the
/// underlying array. It matches C's representation of a raw pointer, so it can be used in any of
/// the extern function argument positions that would take a [`jbyteArray`].
// Note: use `JObject` internally to conveniently derive `Default` so that it can be returned
// instead of `jbyteArray` in `execute_and_catch`.
#[repr(transparent)]
#[derive(Default)]
pub struct JByteArray<'a>(JObject<'a>);

impl<'a> From<jbyteArray> for JByteArray<'a> {
    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn from(inner: jbyteArray) -> Self {
        unsafe { Self(JObject::from_raw(inner)) }
    }
}

impl<'a> JByteArray<'a> {
    fn to_guarded_slice(&self, env: JNIEnv<'a>) -> Result<SliceGuard<'a>> {
        let array = env.get_byte_array_elements(self.0.into_raw(), ReleaseMode::NoCopyBack)?;
        let slice = unsafe { from_raw_parts(array.as_ptr() as *mut u8, array.size()? as usize) };
        Ok(SliceGuard {
            _array: array,
            slice,
        })
    }
}

/// Wrapper around `&[u8]` derived from `jbyteArray` to prevent it from being auto-released.
pub struct SliceGuard<'a> {
    _array: AutoArray<'a, jbyte>,
    slice: &'a [u8],
}

impl<'a> Deref for SliceGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}

#[repr(transparent)]
pub struct Pointer<'a, T> {
    pointer: jlong,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T> Default for Pointer<'a, T> {
    fn default() -> Self {
        Self {
            pointer: 0,
            _phantom: Default::default(),
        }
    }
}

impl<T> From<T> for Pointer<'static, T> {
    fn from(value: T) -> Self {
        Pointer {
            pointer: Box::into_raw(Box::new(value)) as jlong,
            _phantom: PhantomData,
        }
    }
}

impl<T> Pointer<'static, T> {
    fn null() -> Self {
        Pointer {
            pointer: 0,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T> Pointer<'a, T> {
    fn as_ref(&self) -> &'a T {
        debug_assert!(self.pointer != 0);
        unsafe { &*(self.pointer as *const T) }
    }

    fn as_mut(&mut self) -> &'a mut T {
        debug_assert!(self.pointer != 0);
        unsafe { &mut *(self.pointer as *mut T) }
    }

    fn drop(self) {
        debug_assert!(self.pointer != 0);
        unsafe { drop(Box::from_raw(self.pointer as *mut T)) }
    }
}

/// In most Jni interfaces, the first parameter is `JNIEnv`, and the second parameter is `JClass`.
/// This struct simply encapsulates the two common parameters into a single struct for simplicity.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EnvParam<'a> {
    env: JNIEnv<'a>,
    class: JClass<'a>,
}

impl<'a> Deref for EnvParam<'a> {
    type Target = JNIEnv<'a>;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

impl<'a> EnvParam<'a> {
    pub fn get_class(&self) -> JClass<'a> {
        self.class
    }
}

fn execute_and_catch<F, Ret>(env: EnvParam<'_>, inner: F) -> Ret
where
    F: FnOnce() -> Result<Ret>,
    Ret: Default,
{
    match rw_catch_unwind(std::panic::AssertUnwindSafe(inner)) {
        Ok(Ok(ret)) => ret,
        Ok(Err(e)) => {
            match e {
                BindingError::Jni {
                    error: jni::errors::Error::JavaException,
                    backtrace,
                } => {
                    tracing::error!("get JavaException thrown from: {:?}", backtrace);
                    // the exception is already thrown. No need to throw again
                }
                _ => {
                    env.throw(format!("get error while processing: {:?}", e))
                        .expect("should be able to throw");
                }
            }
            Ret::default()
        }
        Err(e) => {
            env.throw(format!("panic while processing: {:?}", e))
                .expect("should be able to throw");
            Ret::default()
        }
    }
}

pub enum JavaBindingRowInner {
    Keyed(KeyedRow),
    StreamChunk(StreamChunkRow),
}
#[derive(Default)]
pub struct JavaClassMethodCache {
    big_decimal_ctor: OnceCell<(GlobalRef, JMethodID)>,
    timestamp_ctor: OnceCell<(GlobalRef, JMethodID)>,

    date_ctor: OnceCell<(GlobalRef, JStaticMethodID)>,
    time_ctor: OnceCell<(GlobalRef, JStaticMethodID)>,
}

pub struct JavaBindingRow {
    inner: JavaBindingRowInner,
    class_cache: Arc<JavaClassMethodCache>,
}

impl JavaBindingRow {
    fn with_stream_chunk(
        underlying: StreamChunkRow,
        class_cache: Arc<JavaClassMethodCache>,
    ) -> Self {
        Self {
            inner: JavaBindingRowInner::StreamChunk(underlying),
            class_cache,
        }
    }

    fn with_keyed(underlying: KeyedRow, class_cache: Arc<JavaClassMethodCache>) -> Self {
        Self {
            inner: JavaBindingRowInner::Keyed(underlying),
            class_cache,
        }
    }

    fn as_keyed(&self) -> &KeyedRow {
        match &self.inner {
            JavaBindingRowInner::Keyed(r) => r,
            _ => unreachable!("can only call as_keyed for KeyedRow"),
        }
    }

    fn as_stream_chunk(&self) -> &StreamChunkRow {
        match &self.inner {
            JavaBindingRowInner::StreamChunk(r) => r,
            _ => unreachable!("can only call as_stream_chunk for StreamChunkRow"),
        }
    }
}

impl Deref for JavaBindingRow {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            JavaBindingRowInner::Keyed(r) => r.row(),
            JavaBindingRowInner::StreamChunk(r) => r.row(),
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_vnodeCount(
    _env: EnvParam<'_>,
) -> jint {
    VirtualNode::COUNT as jint
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_hummockIteratorNew<'a>(
    env: EnvParam<'a>,
    read_plan: JByteArray<'a>,
) -> Pointer<'static, HummockJavaBindingIterator> {
    execute_and_catch(env, move || {
        let read_plan = Message::decode(read_plan.to_guarded_slice(*env)?.deref())?;
        let iter = RUNTIME.block_on(HummockJavaBindingIterator::new(read_plan))?;
        Ok(iter.into())
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_hummockIteratorNext<'a>(
    env: EnvParam<'a>,
    mut pointer: Pointer<'a, HummockJavaBindingIterator>,
) -> Pointer<'static, JavaBindingRow> {
    execute_and_catch(env, move || {
        let iter = pointer.as_mut();
        match RUNTIME.block_on(iter.next())? {
            None => Ok(Pointer::null()),
            Some(row) => Ok(JavaBindingRow::with_keyed(row, iter.class_cache.clone()).into()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_hummockIteratorClose(
    _env: EnvParam<'_>,
    pointer: Pointer<'_, HummockJavaBindingIterator>,
) {
    pointer.drop();
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_streamChunkIteratorNew<'a>(
    env: EnvParam<'a>,
    stream_chunk_payload: JByteArray<'a>,
) -> Pointer<'static, StreamChunkIterator> {
    execute_and_catch(env, move || {
        let prost_stream_chumk =
            Message::decode(stream_chunk_payload.to_guarded_slice(*env)?.deref())?;
        let iter = StreamChunkIterator::new(StreamChunk::from_protobuf(&prost_stream_chumk)?);
        Ok(iter.into())
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_streamChunkIteratorNext<'a>(
    env: EnvParam<'a>,
    mut pointer: Pointer<'a, StreamChunkIterator>,
) -> Pointer<'static, JavaBindingRow> {
    execute_and_catch(env, move || {
        let iter = pointer.as_mut();
        match iter.next() {
            None => Ok(Pointer::null()),
            Some(row) => {
                Ok(JavaBindingRow::with_stream_chunk(row, iter.class_cache.clone()).into())
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_streamChunkIteratorClose(
    _env: EnvParam<'_>,
    pointer: Pointer<'_, StreamChunkIterator>,
) {
    pointer.drop();
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetKey<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
) -> JByteArray<'a> {
    execute_and_catch(env, move || {
        Ok(JByteArray::from(env.byte_array_from_slice(
            pointer.as_ref().as_keyed().key(),
        )?))
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetOp<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
) -> jint {
    execute_and_catch(env, move || {
        Ok(pointer.as_ref().as_stream_chunk().op() as jint)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowIsNull<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> jboolean {
    execute_and_catch(env, move || {
        Ok(pointer.as_ref().datum_at(idx as usize).is_none() as jboolean)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetInt16Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> jshort {
    execute_and_catch(env, move || {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int16())
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetInt32Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> jint {
    execute_and_catch(env, move || {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int32())
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetInt64Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> jlong {
    execute_and_catch(env, move || {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int64())
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetFloatValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> jfloat {
    execute_and_catch(env, move || {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_float32()
            .into())
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetDoubleValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> jdouble {
    execute_and_catch(env, move || {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_float64()
            .into())
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetBooleanValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> jboolean {
    execute_and_catch(env, move || {
        Ok(pointer.as_ref().datum_at(idx as usize).unwrap().into_bool() as jboolean)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetStringValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move || {
        Ok(env.new_string(pointer.as_ref().datum_at(idx as usize).unwrap().into_utf8())?)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetIntervalValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move || {
        let interval = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_interval()
            .as_iso_8601();
        Ok(env.new_string(interval)?)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetJsonbValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move || {
        let jsonb = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_jsonb()
            .to_string();
        Ok(env.new_string(jsonb)?)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetTimestampValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move || {
        let scalar_value = pointer.as_ref().datum_at(idx as usize).unwrap();
        let millis = match scalar_value {
            // supports sinking rw timestamptz (ScalarRefImpl::Int64) to mysql timestamp
            ScalarRefImpl::Int64(v) => v / 1000,
            _ => scalar_value.into_timestamp().0.timestamp_millis(),
        };
        let (ts_class_ref, constructor) = pointer
            .as_ref()
            .class_cache
            .timestamp_ctor
            .get_or_try_init(|| {
                let cls = env.find_class("java/sql/Timestamp")?;
                let init_method = env.get_method_id(cls, "<init>", "(J)V")?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        let ts_class = JClass::from(ts_class_ref.as_obj());
        let date_obj = env.new_object_unchecked(ts_class, *constructor, &[millis.into()])?;

        Ok(date_obj)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetDecimalValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move || {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_decimal()
            .to_string();
        let string_value = env.new_string(value)?;
        let (decimal_class_ref, constructor) = pointer
            .as_ref()
            .class_cache
            .big_decimal_ctor
            .get_or_try_init(|| {
                let cls = env.find_class("java/math/BigDecimal")?;
                let init_method = env.get_method_id(cls, "<init>", "(Ljava/lang/String;)V")?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        let decimal_class = JClass::from(decimal_class_ref.as_obj());
        let date_obj =
            env.new_object_unchecked(decimal_class, *constructor, &[string_value.into()])?;

        Ok(date_obj)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetDateValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move || {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_date()
            .0
            .to_string();

        let string_value = env.new_string(value)?;
        let (class_ref, constructor) =
            pointer.as_ref().class_cache.date_ctor.get_or_try_init(|| {
                let cls = env.find_class("java/sql/Date")?;
                let init_method = env.get_static_method_id(
                    cls,
                    "valueOf",
                    "(Ljava/lang/String;)Ljava/sql/Date;",
                )?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        let class = JClass::from(class_ref.as_obj());
        let JValue::Object(date_obj) = env.call_static_method_unchecked(
            class,
            *constructor,
            ReturnType::Object,
            &[jvalue::from(JValue::from(string_value))],
        )? else {
            return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                name: "valueOf".to_string(),
                sig: "(Ljava/lang/String;)Ljava/sql/Date;".into(),
            }));
        };
        Ok(date_obj)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetTimeValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move || {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_time()
            .0
            .to_string();

        let string_value = env.new_string(value)?;
        let (class_ref, constructor) =
            pointer.as_ref().class_cache.time_ctor.get_or_try_init(|| {
                let cls = env.find_class("java/sql/Time")?;
                let init_method = env.get_static_method_id(
                    cls,
                    "valueOf",
                    "(Ljava/lang/String;)Ljava/sql/Time;",
                )?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        let class = JClass::from(class_ref.as_obj());
        let JValue::Object(obj) = env.call_static_method_unchecked(
            class,
            *constructor,
            ReturnType::Object,
            &[jvalue::from(JValue::from(string_value))],
        )? else {
            return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                name: "valueOf".to_string(),
                sig: "(Ljava/lang/String;)Ljava/sql/Time;".into(),
            }));
        };
        Ok(obj)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetByteaValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move || {
        let bytes = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_bytea();
        let bytes_value = env.byte_array_from_slice(bytes)?;
        unsafe { Ok(JObject::from_raw(bytes_value)) }
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowGetArrayValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
    idx: jint,
    class: JClass<'a>,
) -> JObject<'a> {
    execute_and_catch(env, move || {
        let elems = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_list()
            .iter();

        // convert the Rust elements to a Java object array (Object[])
        let jarray = env.new_object_array(elems.len() as jsize, class, JObject::null())?;

        for (i, ele) in elems.enumerate() {
            let index = i as jsize;
            match ele {
                None => env.set_object_array_element(jarray, i as jsize, JObject::null())?,
                Some(val) => match val {
                    ScalarRefImpl::Int16(v) => {
                        let obj = env.call_static_method(
                            class,
                            "valueOf",
                            "(S)Ljava.lang.Short;",
                            &[JValue::from(v as jshort)],
                        )?;
                        if let JValue::Object(o) = obj {
                            env.set_object_array_element(jarray, index, o)?
                        }
                    }
                    ScalarRefImpl::Int32(v) => {
                        let obj = env.call_static_method(
                            class,
                            "valueOf",
                            "(I)Ljava.lang.Integer;",
                            &[JValue::from(v as jint)],
                        )?;
                        if let JValue::Object(o) = obj {
                            env.set_object_array_element(jarray, index, o)?
                        }
                    }
                    ScalarRefImpl::Int64(v) => {
                        let obj = env.call_static_method(
                            class,
                            "valueOf",
                            "(J)Ljava.lang.Long;",
                            &[JValue::from(v as jlong)],
                        )?;
                        if let JValue::Object(o) = obj {
                            env.set_object_array_element(jarray, index, o)?
                        }
                    }
                    ScalarRefImpl::Float32(v) => {
                        let obj = env.call_static_method(
                            class,
                            "valueOf",
                            "(F)Ljava/lang/Float;",
                            &[JValue::from(v.into_inner() as jfloat)],
                        )?;
                        if let JValue::Object(o) = obj {
                            env.set_object_array_element(jarray, index, o)?
                        }
                    }
                    ScalarRefImpl::Float64(v) => {
                        let obj = env.call_static_method(
                            class,
                            "valueOf",
                            "(D)Ljava/lang/Double;",
                            &[JValue::from(v.into_inner() as jdouble)],
                        )?;
                        if let JValue::Object(o) = obj {
                            env.set_object_array_element(jarray, index, o)?
                        }
                    }
                    ScalarRefImpl::Utf8(v) => {
                        let obj = env.new_string(v)?;
                        env.set_object_array_element(jarray, index, obj)?
                    }
                    _ => env.set_object_array_element(jarray, index, JObject::null())?,
                },
            }
        }
        let output = unsafe { JObject::from_raw(jarray as jobject) };
        Ok(output)
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_rowClose<'a>(
    _env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingRow>,
) {
    pointer.drop()
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_expr::vector_op::cast::literal_parsing;

    /// make sure that the [`ScalarRefImpl::Int64`] received by
    /// [`Java_com_risingwave_java_binding_Binding_rowGetTimestampValue`]
    /// is of type [`DataType::Timestamptz`] stored in microseconds
    #[test]
    fn test_timestamptz_to_i64() {
        let timestamptz_val =
            literal_parsing(&DataType::Timestamptz, "2023-06-01 09:45:00+08:00").unwrap();
        assert_eq!(timestamptz_val.as_int64(), &1_685_583_900_000_000);
    }
}
