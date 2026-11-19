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

mod subscribe_with_route {
    use std::time::Duration;

    use async_nats::{ConnectOptions, Event, Message, Route, Routed};
    use futures_util::StreamExt;
    use tokio::sync::{mpsc, oneshot};

    /// Records every subject it hears, answers each message the same way, and says when it drops.
    struct Recorded {
        heard: mpsc::UnboundedSender<String>,
        answer: Routed,
        dropped: Option<oneshot::Sender<()>>,
    }

    impl Route for Recorded {
        fn deliver(&mut self, message: Message) -> Routed {
            self.heard.send(message.subject.to_string()).ok();
            self.answer
        }
    }

    impl Drop for Recorded {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                dropped.send(()).ok();
            }
        }
    }

    fn recorded(
        answer: Routed,
    ) -> (
        Recorded,
        mpsc::UnboundedReceiver<String>,
        oneshot::Receiver<()>,
    ) {
        let (heard, receiver) = mpsc::unbounded_channel();
        let (dropped, gone) = oneshot::channel();
        let route = Recorded {
            heard,
            answer,
            dropped: Some(dropped),
        };
        (route, receiver, gone)
    }

    async fn next(receiver: &mut mpsc::UnboundedReceiver<String>) -> String {
        tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("a message arrives")
            .expect("the route is alive")
    }

    async fn quiet(receiver: &mut mpsc::UnboundedReceiver<String>) {
        let more = tokio::time::timeout(Duration::from_millis(300), receiver.recv()).await;
        assert!(more.is_err(), "unexpected message {more:?}");
    }

    async fn gone(dropped: oneshot::Receiver<()>) {
        let gone = tokio::time::timeout(Duration::from_secs(5), dropped).await;
        assert_eq!(gone.expect("the route drops in time"), Ok(()));
    }

    #[tokio::test]
    async fn the_route_takes_every_message_in_the_order_it_was_published() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (route, mut heard, _dropped) = recorded(Routed::Taken);
        let _handle = client.subscribe_with_route("r.>", route).await.unwrap();
        client.flush().await.unwrap();

        for index in 0..100 {
            client
                .publish(format!("r.{index}"), "data".into())
                .await
                .unwrap();
        }

        for index in 0..100 {
            assert_eq!(next(&mut heard).await, format!("r.{index}"));
        }
        quiet(&mut heard).await;
    }

    #[tokio::test]
    async fn dropping_the_handle_unsubscribes_and_drops_the_route() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (route, mut heard, dropped) = recorded(Routed::Taken);
        let handle = client.subscribe_with_route("data", route).await.unwrap();
        client.flush().await.unwrap();

        drop(handle);
        gone(dropped).await;
        client.publish("data", "data".into()).await.unwrap();

        let ended = tokio::time::timeout(Duration::from_secs(5), heard.recv()).await;
        assert_eq!(ended.expect("the record ends in time"), None);
    }

    #[tokio::test]
    async fn a_route_that_closes_is_unsubscribed_and_dropped_after_that_message() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (route, mut heard, dropped) = recorded(Routed::Closed);
        let _handle = client.subscribe_with_route("data", route).await.unwrap();
        client.flush().await.unwrap();

        client.publish("data", "data".into()).await.unwrap();

        assert_eq!(next(&mut heard).await, "data");
        gone(dropped).await;
        client.publish("data", "data".into()).await.unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(5), heard.recv()).await;
        assert_eq!(ended.expect("the record ends in time"), None);
    }

    #[tokio::test]
    async fn a_full_route_reports_its_sid_as_a_slow_consumer_and_keeps_receiving() {
        let server = nats_server::run_basic_server();
        let (events, mut slow) = mpsc::channel(128);
        let client = ConnectOptions::new()
            .event_callback(move |event| {
                let events = events.clone();
                async move {
                    if let Event::SlowConsumer(sid) = event {
                        events.send(sid).await.unwrap();
                    }
                }
            })
            .connect(server.client_url())
            .await
            .unwrap();
        let (route, mut heard, _dropped) = recorded(Routed::Full);
        let handle = client.subscribe_with_route("data", route).await.unwrap();
        client.flush().await.unwrap();

        client.publish("data", "first".into()).await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), slow.recv()).await;
        assert_eq!(event.unwrap(), Some(handle.sid()));

        client.publish("data", "second".into()).await.unwrap();
        assert_eq!(next(&mut heard).await, "data");
        assert_eq!(next(&mut heard).await, "data");
    }

    #[tokio::test]
    async fn a_route_survives_a_reconnect() {
        let mut server = nats_server::run_server_with_port("", Some("5762"));
        let (events, mut connected) = mpsc::channel(128);
        let client = ConnectOptions::new()
            .event_callback(move |event| {
                let events = events.clone();
                async move {
                    if let Event::Connected = event {
                        events.send(()).await.unwrap();
                    }
                }
            })
            .connect(server.client_url())
            .await
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), connected.recv()).await;
        first.unwrap().unwrap();
        let (route, mut heard, _dropped) = recorded(Routed::Taken);
        let _handle = client.subscribe_with_route("kept", route).await.unwrap();
        client.flush().await.unwrap();

        server.restart();
        let again = tokio::time::timeout(Duration::from_secs(10), connected.recv()).await;
        again.unwrap().unwrap();
        client.flush().await.unwrap();
        client.publish("kept", "data".into()).await.unwrap();

        assert_eq!(next(&mut heard).await, "kept");
    }

    #[tokio::test]
    async fn draining_the_client_drops_the_route() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (route, _heard, dropped) = recorded(Routed::Taken);
        let _handle = client.subscribe_with_route("data", route).await.unwrap();
        client.flush().await.unwrap();

        client.drain().await.unwrap();

        gone(dropped).await;
    }

    #[tokio::test]
    async fn a_route_and_a_plain_subscriber_on_one_subject_both_receive() {
        let server = nats_server::run_basic_server();
        let client = async_nats::connect(server.client_url()).await.unwrap();
        let (route, mut heard, _dropped) = recorded(Routed::Taken);
        let _handle = client.subscribe_with_route("data", route).await.unwrap();
        let mut plain = client.subscribe("data").await.unwrap();
        client.flush().await.unwrap();

        client.publish("data", "data".into()).await.unwrap();

        assert_eq!(next(&mut heard).await, "data");
        let message = tokio::time::timeout(Duration::from_secs(5), plain.next()).await;
        assert_eq!(message.unwrap().unwrap().subject.as_str(), "data");
    }
}
