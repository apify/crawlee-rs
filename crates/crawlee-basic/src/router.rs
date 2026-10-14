//! Routes requests to handlers by `request.user_data.label`.

use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;

use serde::de::DeserializeOwned;

use crawlee_core::errors::NonRetryableError;

use crate::context::CrawlingContext;
use crate::errors::MissingRouteError;
use crate::handler::{BoxFuture, RequestHandler};

type Route<C> = Rc<dyn RequestHandler<C>>;

/// Dispatches requests to handlers by label, falling back to the default handler.
///
/// ```
/// use crawlee_basic::{BasicContext, Router};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Detail {
///     category: String,
/// }
///
/// let mut router = Router::<BasicContext>::new();
/// router.add_default_handler(|ctx: BasicContext| async move {
///     println!("listing {}", ctx.request().url);
///     Ok(())
/// });
/// router.add_typed_handler("DETAIL", |ctx: BasicContext, data: Detail| async move {
///     println!("{} in {}", ctx.request().url, data.category);
///     Ok(())
/// });
/// ```
pub struct Router<C> {
    routes: HashMap<String, Route<C>>,
    default: Option<Route<C>>,
}

impl<C> Default for Router<C> {
    fn default() -> Self {
        Router { routes: HashMap::new(), default: None }
    }
}

impl<C: CrawlingContext> Router<C> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_handler(&mut self, label: impl Into<String>, handler: impl RequestHandler<C>) -> &mut Self {
        self.routes.insert(label.into(), Rc::new(handler));
        self
    }

    pub fn add_default_handler(&mut self, handler: impl RequestHandler<C>) -> &mut Self {
        self.default = Some(Rc::new(handler));
        self
    }

    /// Adds a handler that receives the request's `user_data` deserialized into `T`. A request
    /// whose data does not match fails without retries, like a failed schema validation in
    /// Crawlee for JS.
    pub fn add_typed_handler<T, F, Fut>(&mut self, label: impl Into<String>, handler: F) -> &mut Self
    where
        T: DeserializeOwned + 'static,
        F: Fn(C, T) -> Fut + 'static,
        Fut: Future<Output = anyhow::Result<()>> + 'static,
    {
        let label = label.into();
        let route_label = label.clone();
        let handler = Rc::new(handler);
        self.add_handler(label, move |ctx: C| {
            let handler = handler.clone();
            let parsed = ctx.basic().request().user_data_as::<T>();
            let route_label = route_label.clone();
            async move {
                let data = parsed.map_err(|err| {
                    NonRetryableError::new(format!("Invalid userData for route '{route_label}': {err}"))
                })?;
                handler(ctx, data).await
            }
        })
    }

    fn route_for(&self, label: Option<&str>) -> Result<&Route<C>, MissingRouteError> {
        label
            .and_then(|label| self.routes.get(label))
            .or(self.default.as_ref())
            .ok_or_else(|| MissingRouteError { label: label.unwrap_or("undefined").to_owned() })
    }
}

impl<C: CrawlingContext> RequestHandler<C> for Router<C> {
    fn handle(&self, ctx: C) -> BoxFuture<anyhow::Result<()>> {
        match self.route_for(ctx.basic().request().label()) {
            Ok(route) => route.handle(ctx),
            Err(err) => Box::pin(async move { Err(err.into()) }),
        }
    }
}
