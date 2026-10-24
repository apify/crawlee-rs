//! Request handlers, error handlers and the context pipeline.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::context::BasicContext;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Handles one request. Implemented for async closures `|ctx| async move { ... }` and for
/// [`Router`](crate::Router).
pub trait RequestHandler<C>: Send + Sync + 'static {
    fn handle(&self, ctx: C) -> BoxFuture<anyhow::Result<()>>;

    /// The timeout of requests with this label, when the handler overrides the crawler's
    /// `request_handler_timeout` (a [`Router`](crate::Router) route can).
    fn timeout_for(&self, _label: Option<&str>) -> Option<std::time::Duration> {
        None
    }

    /// The longest timeout [`timeout_for`](Self::timeout_for) returns for any label.
    fn max_timeout(&self) -> Option<std::time::Duration> {
        None
    }
}

impl<C, F, Fut> RequestHandler<C> for F
where
    C: Send + 'static,
    F: Fn(C) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    fn handle(&self, ctx: C) -> BoxFuture<anyhow::Result<()>> {
        Box::pin(self(ctx))
    }
}

/// Called with the basic context and the error, before a retry (`error_handler`) or after the
/// last attempt failed (`failed_request_handler`). Changes to the request are kept.
pub trait ErrorHandler: Send + Sync + 'static {
    fn handle(&self, ctx: BasicContext, error: Arc<anyhow::Error>) -> BoxFuture<anyhow::Result<()>>;
}

impl<F, Fut> ErrorHandler for F
where
    F: Fn(BasicContext, Arc<anyhow::Error>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    fn handle(&self, ctx: BasicContext, error: Arc<anyhow::Error>) -> BoxFuture<anyhow::Result<()>> {
        Box::pin(self(ctx, error))
    }
}

/// A step of the context pipeline: turns the context built so far into a richer one (for
/// example, `BasicContext` into `HttpContext` by fetching the page).
///
/// Middlewares compose with [`Then`]; the whole pipeline is a single `Middleware<BasicContext>`
/// whose output is the context the request handler receives. Types are checked at compile time:
/// a step can only follow a step whose output it accepts.
#[async_trait]
pub trait Middleware<In: Send + 'static>: Send + Sync + 'static {
    type Out: Send + 'static;
    async fn run(&self, input: In) -> anyhow::Result<Self::Out>;

    /// The longest this step may take to navigate (`getNavigationTimeoutMillis`). The crawler adds
    /// it to the handler timeout, so the request timeout does not cut the navigation short. Zero
    /// for steps that do not navigate.
    fn navigation_timeout(&self) -> Duration {
        Duration::ZERO
    }
}

/// The empty pipeline.
#[derive(Clone, Copy, Debug, Default)]
pub struct Identity;

#[async_trait]
impl<In: Send + 'static> Middleware<In> for Identity {
    type Out = In;
    async fn run(&self, input: In) -> anyhow::Result<In> {
        Ok(input)
    }
}

/// Runs `A`, then `B` on its output.
#[derive(Clone, Copy, Debug, Default)]
pub struct Then<A, B>(pub A, pub B);

#[async_trait]
impl<In, A, B> Middleware<In> for Then<A, B>
where
    In: Send + 'static,
    A: Middleware<In>,
    B: Middleware<A::Out>,
{
    type Out = B::Out;
    async fn run(&self, input: In) -> anyhow::Result<B::Out> {
        let intermediate = self.0.run(input).await?;
        self.1.run(intermediate).await
    }

    fn navigation_timeout(&self) -> Duration {
        self.0.navigation_timeout() + self.1.navigation_timeout()
    }
}

/// A middleware from an async closure.
pub struct FnMiddleware<F>(pub F);

#[async_trait]
impl<In, Out, F, Fut> Middleware<In> for FnMiddleware<F>
where
    In: Send + 'static,
    Out: Send + 'static,
    F: Fn(In) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<Out>> + Send + 'static,
{
    type Out = Out;
    async fn run(&self, input: In) -> anyhow::Result<Out> {
        (self.0)(input).await
    }
}
