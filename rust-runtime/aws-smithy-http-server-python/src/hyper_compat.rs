/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{error::Error as StdError, io, task::{Context, Poll}};

use aws_smithy_legacy_http_server::body::{
    Body as LegacyBody, HttpBody as LegacyHttpBody,
};
use bytes::Bytes;
use futures_util::{future::BoxFuture, FutureExt, Stream};
use http::{
    header::{
        HeaderMap as HeaderMap0, HeaderName as HeaderName0, HeaderValue as HeaderValue0,
    },
    Method as Method0, Request as Request0, Response as Response0, StatusCode as StatusCode0,
    Uri as Uri0, Version as Version0,
};
use hyper::{
    body::{Body as HyperBody, Frame as HyperFrame, Incoming},
    header::{
        HeaderMap as HeaderMap1, HeaderName as HeaderName1, HeaderValue as HeaderValue1,
    },
    Method as Method1, Request as Request1, Response as Response1, StatusCode as StatusCode1,
    Uri as Uri1, Version as Version1,
};
use pin_project_lite::pin_project;
use tower::{Service, ServiceExt};

pin_project! {
    /// Stream adapter that turns a hyper 1 body into bytes for legacy bodies.
    pub struct HyperBodyToLegacyStream<B> {
        #[pin]
        body: B,
    }
}

impl<B> HyperBodyToLegacyStream<B> {
    pub fn new(body: B) -> Self {
        Self { body }
    }
}

impl<B> Stream for HyperBodyToLegacyStream<B>
where
    B: HyperBody<Data = Bytes>,
{
    type Item = Result<Bytes, io::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, io::Error>>> {
        let mut this = self.project();

        loop {
            match this.body.as_mut().poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => return Poll::Ready(Some(Ok(data))),
                    Err(_) => continue,
                },
                Poll::Ready(Some(Err(err))) => {
                    let _ = err;
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "hyper body error",
                    ))))
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    /// Body adapter that turns a legacy `http_body 0.4` body into a hyper 1 body.
    pub struct LegacyBodyToHyperBody<B> {
        #[pin]
        body: B,
    }
}

impl<B> LegacyBodyToHyperBody<B> {
    pub fn new(body: B) -> Self {
        Self { body }
    }
}

impl<B> HyperBody for LegacyBodyToHyperBody<B>
where
    B: LegacyHttpBody<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<HyperFrame<Self::Data>, Self::Error>>> {
        let mut this = self.project();

        loop {
            match this.body.as_mut().poll_data(cx) {
                Poll::Ready(Some(Ok(data))) => {
                    return Poll::Ready(Some(Ok(HyperFrame::data(data))));
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => break,
                Poll::Pending => return Poll::Pending,
            }
        }

        match this.body.as_mut().poll_trailers(cx) {
            Poll::Ready(Ok(Some(trailers))) => {
                Poll::Ready(Some(Ok(HyperFrame::trailers(convert_headers_0x_to_1x(
                    trailers,
                )))))
            }
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        let size_hint = self.body.size_hint();
        let mut out = hyper::body::SizeHint::new();
        out.set_lower(size_hint.lower());
        if let Some(upper) = size_hint.upper() {
            out.set_upper(upper);
        }
        out
    }
}

/// Adapter that turns a tower service using legacy http types into a hyper 1 HTTP service.
#[derive(Debug, Clone)]
pub struct LegacyTowerServiceAsHyper1Service<S> {
    service: S,
}

impl<S> LegacyTowerServiceAsHyper1Service<S> {
    pub fn new(service: S) -> Self {
        Self { service }
    }
}

impl<S, ResBody> hyper::service::Service<Request1<Incoming>> for LegacyTowerServiceAsHyper1Service<S>
where
    S: Service<Request0<LegacyBody>, Response = Response0<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ResBody: LegacyHttpBody<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Response = Response1<LegacyBodyToHyperBody<ResBody>>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request1<Incoming>) -> Self::Future {
        let service = self.service.clone();
        async move {
            let req = convert_request(req);
            let res = service.oneshot(req).await?;
            Ok(convert_response(res))
        }
        .boxed()
    }
}

fn convert_request(req: Request1<Incoming>) -> Request0<LegacyBody> {
    let (parts, body) = req.into_parts();
    let body = LegacyBody::wrap_stream(HyperBodyToLegacyStream::new(body));
    let mut builder = Request0::builder()
        .method(convert_method_1_to_0(parts.method))
        .uri(convert_uri_1_to_0(&parts.uri))
        .version(convert_version_1_to_0(parts.version));

    let mut last_name: Option<HeaderName1> = None;
    for (name, value) in parts.headers.into_iter() {
        let name = name
            .or_else(|| last_name.clone())
            .expect("header name should be present");
        builder = builder.header(name.as_str(), value.as_bytes());
        last_name = Some(name);
    }

    builder.body(body).expect("request body conversion should succeed")
}

fn convert_response<ResBody>(res: Response0<ResBody>) -> Response1<LegacyBodyToHyperBody<ResBody>>
where
    ResBody: LegacyHttpBody<Data = Bytes>,
{
    let (parts, body) = res.into_parts();
    let mut builder = Response1::builder()
        .status(convert_status_0_to_1(parts.status))
        .version(convert_version_0_to_1(parts.version));

    let mut last_name: Option<HeaderName0> = None;
    for (name, value) in parts.headers.into_iter() {
        let name = name
            .or_else(|| last_name.clone())
            .expect("header name should be present");
        builder = builder.header(name.as_str(), value.as_bytes());
        last_name = Some(name);
    }

    builder
        .body(LegacyBodyToHyperBody::new(body))
        .expect("response body conversion should succeed")
}

fn convert_method_1_to_0(method: Method1) -> Method0 {
    Method0::from_bytes(method.as_str().as_bytes()).expect("valid HTTP method")
}

fn convert_uri_1_to_0(uri: &Uri1) -> Uri0 {
    uri.to_string().parse().expect("valid legacy URI")
}

fn convert_version_1_to_0(version: Version1) -> Version0 {
    match version {
        Version1::HTTP_09 => Version0::HTTP_09,
        Version1::HTTP_10 => Version0::HTTP_10,
        Version1::HTTP_11 => Version0::HTTP_11,
        Version1::HTTP_2 => Version0::HTTP_2,
        Version1::HTTP_3 => Version0::HTTP_11,
        _ => Version0::HTTP_11,
    }
}

fn convert_status_0_to_1(status: StatusCode0) -> StatusCode1 {
    StatusCode1::from_u16(status.as_u16()).expect("valid status code")
}

fn convert_version_0_to_1(version: Version0) -> Version1 {
    match version {
        Version0::HTTP_09 => Version1::HTTP_09,
        Version0::HTTP_10 => Version1::HTTP_10,
        Version0::HTTP_11 => Version1::HTTP_11,
        Version0::HTTP_2 => Version1::HTTP_2,
        _ => Version1::HTTP_11,
    }
}

fn convert_headers_0x_to_1x(input: HeaderMap0) -> HeaderMap1 {
    let mut map = HeaderMap1::new();
    let mut last_name: Option<HeaderName0> = None;
    for (name, value) in input.into_iter() {
        let name = name
            .or_else(|| last_name.clone())
            .expect("header name should be present");
        map.append(
            HeaderName1::from_bytes(name.as_str().as_bytes()).expect("valid header name"),
            HeaderValue1::from_bytes(value.as_bytes()).expect("valid header value"),
        );
        last_name = Some(name);
    }
    map
}

#[allow(dead_code)]
fn convert_headers_1x_to_0x(input: HeaderMap1) -> HeaderMap0 {
    let mut map = HeaderMap0::new();
    let mut last_name: Option<HeaderName1> = None;
    for (name, value) in input.into_iter() {
        let name = name
            .or_else(|| last_name.clone())
            .expect("header name should be present");
        map.append(
            HeaderName0::from_bytes(name.as_str().as_bytes()).expect("valid header name"),
            HeaderValue0::from_bytes(value.as_bytes()).expect("valid header value"),
        );
        last_name = Some(name);
    }
    map
}
