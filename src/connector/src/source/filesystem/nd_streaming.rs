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

use std::io::BufRead;

use bytes::BytesMut;
use futures_async_stream::try_stream;

use crate::source::{BoxSourceStream, SourceMessage};

#[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
/// This function splits a byte stream by the newline character '\n' into a message stream.
/// It can be difficult to split and compute offsets correctly when the bytes are received in
/// chunks.  There are two cases to consider:
/// - When a bytes chunk does not end with '\n', we should not treat the last segment as a new line
///   message, but keep it for the next chunk, and insert it before next chunk's first line
///   beginning.
/// - When a bytes chunk ends with '\n', there is no additional action required.
pub async fn split_stream(data_stream: BoxSourceStream) {
    let mut buf = BytesMut::new();

    let mut last_message = None;
    #[for_await]
    for batch in data_stream {
        let batch = batch?;

        if batch.is_empty() {
            continue;
        }

        // Never panic because we check batch is not empty
        let (offset, split_id, meta) = batch
            .first()
            .map(|msg| (msg.offset.clone(), msg.split_id.clone(), msg.meta.clone()))
            .unwrap();

        let mut offset: usize = offset.parse()?;

        // Never panic because we check batch is not empty
        let last_item = batch.last().unwrap();
        let end_offset: usize = last_item.offset.parse::<usize>().unwrap()
            + last_item
                .payload
                .as_ref()
                .map(|p| p.len())
                .unwrap_or_default();
        for msg in batch {
            let payload = msg.payload.unwrap_or_default();
            buf.extend(payload);
        }
        let mut msgs = Vec::new();
        for (i, line) in buf.lines().enumerate() {
            let mut line = line?;

            // Insert the trailing of the last chunk in front of the first line, do not count
            // the length here.
            if i == 0 && last_message.is_some() {
                let msg: SourceMessage = std::mem::take(&mut last_message).unwrap();
                let last_payload = msg.payload.unwrap();
                offset -= last_payload.len();
                line = String::from_utf8(last_payload).unwrap() + &line;
            }
            let len = line.as_bytes().len();

            msgs.push(SourceMessage {
                payload: Some(line.into()),
                offset: (offset + len).to_string(),
                split_id: split_id.clone(),
                meta: meta.clone(),
            });
            offset += len;
            offset += 1;
        }

        if offset > end_offset {
            last_message = msgs.pop();
        }

        if !msgs.is_empty() {
            yield msgs;
        }

        buf.clear();
    }
    if let Some(msg) = last_message {
        yield vec![msg];
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{StreamExt, TryStreamExt};

    use super::*;

    #[tokio::test]
    async fn test_split_stream() {
        const N1: usize = 10000;
        const N2: usize = 500;
        const N3: usize = 50;
        let lines = (0..N1)
            .map(|x| (0..x % N2).map(|_| 'A').collect::<String>())
            .collect::<Vec<_>>();
        let total_chars = lines.iter().map(|e| e.len()).sum::<usize>();
        let text = lines.join("\n").into_bytes();
        let split_id: Arc<str> = "1".to_string().into_boxed_str().into();
        let s = text
            .chunks(N2)
            .enumerate()
            .map(move |(i, e)| {
                Ok(e.chunks(N3)
                    .enumerate()
                    .map(|(j, buf)| SourceMessage {
                        payload: Some(buf.to_owned()),
                        offset: (i * N2 + j * N3).to_string(),
                        split_id: split_id.clone(),
                        meta: crate::source::SourceMeta::Empty,
                    })
                    .collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(s).boxed();
        let msg_stream = split_stream(stream).try_collect::<Vec<_>>().await.unwrap();
        let items = msg_stream
            .into_iter()
            .flatten()
            .map(|e| String::from_utf8(e.payload.unwrap()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(items.len(), N1);
        let text = items.join("");
        assert_eq!(text.len(), total_chars);
    }
}
