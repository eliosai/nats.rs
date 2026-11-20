// Copyright 2020-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod publish_batch {
    use std::time::Duration;

    use async_nats::message::OutboundMessage;
    use async_nats::{HeaderMap, PublishErrorKind, Subscriber};
    use bytes::Bytes;
    use futures_util::StreamExt;

    fn outbound(subject: &str, payload: &str) -> OutboundMessage {
        OutboundMessage {
            subject: subject.into(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            reply: None,
            headers: None,
        }
    }

    async fn payloads(subscriber: &mut Subscriber, count: usize) -> Vec<String> {
        let mut heard = Vec::with_capacity(count);
        for _ in 0..count {
            let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
                .await
                .expect("a message arrives")
                .expect("the subscription is open");
            heard.push(String::from_utf8(message.payload.to_vec()).unwrap());
        }
        heard
    }

    async fn quiet(subscriber: &mut Subscriber) {
        let more = tokio::time::timeout(Duration::from_millis(300), subscriber.next()).await;
        assert!(more.is_err(), "unexpected message {more:?}");
    }

    #[tokio::test]
    async fn a_batch_arrives_in_order_between_the_publishes_around_it() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let mut subscriber = client.subscribe("data").await.unwrap();
        client.flush().await.unwrap();

        client.publish("data", "before".into()).await.unwrap();
        let mut batch = async_nats::Batch::with_capacity(64);
        for index in 0..64 {
            client
                .batch(&mut batch, outbound("data", &index.to_string()))
                .unwrap();
        }
        client.publish_batch(batch).await.unwrap();
        client.publish("data", "after".into()).await.unwrap();

        let mut expected = vec!["before".to_owned()];
        expected.extend((0..64).map(|index: u32| index.to_string()));
        expected.push("after".to_owned());
        assert_eq!(payloads(&mut subscriber, 66).await, expected);
        quiet(&mut subscriber).await;
    }

    #[tokio::test]
    async fn a_batch_carries_each_message_reply_and_headers() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let mut subscriber = client.subscribe("data").await.unwrap();
        client.flush().await.unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("kind", "item");
        let carried = OutboundMessage {
            subject: "data".into(),
            payload: Bytes::from_static(b"carried"),
            reply: Some("answer".into()),
            headers: Some(headers.clone()),
        };

        let mut batch = async_nats::Batch::default();
        client.batch(&mut batch, carried).unwrap();
        client.publish_batch(batch).await.unwrap();

        let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next()).await;
        let message = message.unwrap().unwrap();
        let reply = message.reply.as_ref().map(|reply| reply.as_str());
        assert_eq!(
            (reply, message.headers, message.payload),
            (
                Some("answer"),
                Some(headers),
                Bytes::from_static(b"carried")
            )
        );
    }

    #[tokio::test]
    async fn an_oversized_message_is_refused_as_it_joins_and_the_batch_keeps_the_rest() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let mut subscriber = client.subscribe("data").await.unwrap();
        client.flush().await.unwrap();
        let oversized = "x".repeat(client.server_info().max_payload + 1);
        let mut batch = async_nats::Batch::default();

        client.batch(&mut batch, outbound("data", "first")).unwrap();
        let refused = client.batch(&mut batch, outbound("data", &oversized));
        client.publish_batch(batch).await.unwrap();

        assert_eq!(
            refused.map_err(|refused| refused.kind()),
            Err(PublishErrorKind::MaxPayloadExceeded)
        );
        assert_eq!(payloads(&mut subscriber, 1).await, vec!["first".to_owned()]);
        quiet(&mut subscriber).await;
    }

    #[tokio::test]
    async fn an_invalid_subject_is_refused_as_it_joins_and_the_batch_keeps_the_rest() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let mut subscriber = client.subscribe("data").await.unwrap();
        client.flush().await.unwrap();
        let mut batch = async_nats::Batch::default();

        client.batch(&mut batch, outbound("data", "first")).unwrap();
        let refused = client.batch(&mut batch, outbound("bad subject", "second"));
        client.publish_batch(batch).await.unwrap();

        assert_eq!(
            refused.map_err(|refused| refused.kind()),
            Err(PublishErrorKind::InvalidSubject)
        );
        assert_eq!(payloads(&mut subscriber, 1).await, vec!["first".to_owned()]);
        quiet(&mut subscriber).await;
    }

    #[tokio::test]
    async fn checking_an_outbound_message_names_the_one_a_batch_would_refuse() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let max_payload = client.server_info().max_payload;
        let fits = "x".repeat(max_payload);
        let oversized = "x".repeat(max_payload + 1);

        let checked = [
            outbound("data", &fits),
            outbound("data", &oversized),
            outbound("bad subject", "second"),
        ]
        .map(|message| {
            client
                .check_outbound(&message)
                .map_err(|refused| refused.kind())
        });

        assert_eq!(
            checked,
            [
                Ok(()),
                Err(PublishErrorKind::MaxPayloadExceeded),
                Err(PublishErrorKind::InvalidSubject),
            ]
        );
    }

    #[tokio::test]
    async fn an_empty_batch_sends_nothing() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let mut subscriber = client.subscribe("data").await.unwrap();
        client.flush().await.unwrap();

        client
            .publish_batch(async_nats::Batch::default())
            .await
            .unwrap();

        client.flush().await.unwrap();
        quiet(&mut subscriber).await;
    }
}
