//! Request handlers, error handlers and the context pipeline.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use async_trait::async_trait;

use crate::context::BasicContext;

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + 'static>>;

/// Handles one request. Implemented for async closures `|ctx| async move { ... }` and for
/// [`Router`](crate::Router).
pub trait RequestHandler<C>: 'static {
    fn handle(&self, ctx: C) -> BoxFuture<anyhow::Result<()>>;
}

impl<C, F, Fut> RequestHandler<C> for F
where
    C: 'static,
    F: Fn(C) -> Fut + 'static,
    Fut: Future<Output = anyhow::Result<()>> + 'static,
{
    fn handle(&self, ctx: C) -> BoxFuture<anyhow::Result<()>> {
        Box::pin(self(ctx))
    }
}

/// Called with the basic context and the error, before a retry (`error_handler`) or after the
/// last attempt failed (`failed_request_handler`). Changes to the request are kept.
pub trait ErrorHandler: 'static {
    fn handle(&self, ctx: BasicContext, error: Rc<anyhow::Error>) -> BoxFuture<anyhow::Result<()>>;
}

impl<F, Fut> ErrorHandler for F
where
    F: Fn(BasicContext, Rc<anyhow::Error>) -> Fut + 'static,
    Fut: Future<Output = anyhow::Result<()>> + 'static,
{
    fn handle(&self, ctx: BasicContext, error: Rc<anyhow::Error>) -> BoxFuture<anyhow::Result<()>> {
        Box::pin(self(ctx, error))
    }
}

/// A step of the context pipeline: turns the context built so far into a richer one (for
/// example, `BasicContext` into `HttpContext` by fetching the page).
///
/// Middlewares compose with [`Then`]; the whole pipeline is a single `Middleware<BasicContext>`
/// whose output is the context the request handler receives. Types are checked at compile time:
/// a step can only follow a step whose output it accepts.
#[async_trait(?Send)]
pub trait Middleware<In: 'static>: 'static {
    type Out: 'static;
    async fn run(&self, input: In) -> anyhow::Result<Self::Out>;
}

/// The empty pipeline.
#[derive(Clone, Copy, Debug, Default)]
pub struct Identity;

#[async_trait(?Send)]
impl<In: 'static> Middleware<In> for Identity {
    type Out = In;
    async fn run(&self, input: In) -> anyhow::Result<In> {
        Ok(input)
    }
}

/// Runs `A`, then `B` on its output.
#[derive(Clone, Copy, Debug, Default)]
pub struct Then<A, B>(pub A, pub B);

#[async_trait(?Send)]
impl<In, A, B> Middleware<In> for Then<A, B>
where
    In: 'static,
    A: Middleware<In>,
    B: Middleware<A::Out>,
{
    type Out = B::Out;
    async fn run(&self, input: In) -> anyhow::Result<B::Out> {
        let intermediate = self.0.run(input).await?;
        self.1.run(intermediate).await
    }
}

/// A middleware from an async closure.
pub struct FnMiddleware<F>(pub F);

#[async_trait(?Send)]
impl<In, Out, F, Fut> Middleware<In> for FnMiddleware<F>
where
    In: 'static,
    Out: 'static,
    F: Fn(In) -> Fut + 'static,
    Fut: Future<Output = anyhow::Result<Out>> + 'static,
{
    type Out = Out;
    async fn run(&self, input: In) -> anyhow::Result<Out> {
        (self.0)(input).await
    }
}
