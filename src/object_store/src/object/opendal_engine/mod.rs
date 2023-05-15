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

pub mod opendal_object_store;
pub use opendal_object_store::*;

pub mod hdfs;
pub use hdfs::*;

pub mod webhdfs;
pub use webhdfs::*;
pub mod gcs;
pub use gcs::*;
pub mod oss;
pub use oss::*;
pub mod azblob;
pub use azblob::*;
pub mod fs;
pub use fs::*;
