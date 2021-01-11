# Copyright 2018 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading
import functools
import structlog

from twisted.internet import defer
from opentracing import global_tracer, tags, Span
from jaeger_client import Span as JaegerSpan, SpanContext as JaegerSpanContext

ROOT_SPAN_KEY_NAME = "op-name"

log = structlog.get_logger()


class _TracingSupport:
    def __init__(self):
        self._log_correlation_enabled = None
        self._trace_publishing_enabled = None
        self._component_name = None
        self._active_trace_agent_address = None
        self._scope_manager = None

    @property
    def log_correlation_status(self):
        return self._log_correlation_enabled is not None and self._log_correlation_enabled

    @log_correlation_status.setter
    def log_correlation_status(self, value):
        if not isinstance(value, bool):
            raise TypeError('Boolean required')
        if self._log_correlation_enabled is not None:
            raise ValueError('Log Correlation has already been set to {}'.format(self._log_correlation_enabled))

        self._log_correlation_enabled = value

    @property
    def trace_publishing_status(self):
        return self._trace_publishing_enabled is not None and self._trace_publishing_enabled

    @trace_publishing_status.setter
    def trace_publishing_status(self, value):
        if not isinstance(value, bool):
            raise TypeError('Boolean required')
        if self._trace_publishing_enabled is not None:
            raise ValueError('Trace publishing has already been set to {}'.format(self._trace_publishing_enabled))

        self._trace_publishing_enabled = value

    @property
    def component_name(self):
        return self._component_name is not None and self._component_name

    @component_name.setter
    def component_name(self, value):
        if not isinstance(value, str):
            raise TypeError('String required')
        if self._component_name is not None:
            raise ValueError('Component name has already been set to {}'.format(self._component_name))

        self._component_name = value

    @property
    def active_trace_agent_address(self):
        return self._active_trace_agent_address is not None and self._active_trace_agent_address

    @active_trace_agent_address.setter
    def active_trace_agent_address(self, value):
        if not isinstance(value, str):
            raise TypeError('String required')
        if self._active_trace_agent_address is not None:
            raise ValueError('Active Trace Agent address has already been set to {}'.
                             format(self._active_trace_agent_address))

        self._active_trace_agent_address = value

    @property
    def scope_manager(self):
        return self._scope_manager is not None and self._scope_manager

    @scope_manager.setter
    def scope_manager(self, value):
        if self._scope_manager is None:
            self._scope_manager = value


GlobalTracingSupport = _TracingSupport()


class NilSpan(Span):
    """ Span that is not an nil span """
    def __init__(self):
        super().__init__(None, None)

    def __str__(self):
        return 'NilSpan: ' + super().__str__()


class NilScope:
    """ Scope that is not an opentrace scope """
    def __init__(self):
        self.span = NilSpan()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        NilSpan._on_error(self.span, exc_type, exc_val, exc_tb)

    def __str__(self):
        return 'NilScope'


def create_async_span(_span_name, _ignore_active_span=False):
    return None

    # // Creates a Async Child Span with Follows-From relationship from Parent Span embedded in passed context.
    # // Should be used only in scenarios when
    # // a) There is dis-continuation in execution and thus result of Child span does not affect the Parent flow at all
    # // b) The execution of Child Span is guaranteed to start after the completion of Parent Span
    # // In case of any confusion, use CreateChildSpan method
    # // Some situations where this method would be suitable includes Kafka Async RPC call, Propagation of Event across
    # // a channel etc.
    # func CreateAsyncSpan(ctx context.Context, taskName string, keyAndValues ...Fields) (opentracing.Span, context.Context) {
    #     if !GetGlobalLFM().GetLogCorrelationStatus() && !GetGlobalLFM().GetTracePublishingStatus() {
    #         return opentracing.NoopTracer{}.StartSpan(taskName), ctx
    #     }
    #
    #     var asyncSpan opentracing.Span
    #     var newCtx context.Context
    #
    #     parentSpan := opentracing.SpanFromContext(ctx)
    #
    #     // We should always be creating Aysnc span from a Valid parent span. If not, create a Child span instead
    #     if parentSpan == nil {
    #         logger.Warn(context.Background(), "Async span must be created with a Valid parent span only")
    #         asyncSpan, newCtx = opentracing.StartSpanFromContext(ctx, taskName)
    #     } else {
    #         // Use Background context as the base for Follows-from case; else new span is getting both Child and FollowsFrom relationship
    #         asyncSpan, newCtx = opentracing.StartSpanFromContext(context.Background(), taskName, opentracing.FollowsFrom(parentSpan.Context()))
    #     }
    #
    #     if parentSpan == nil || parentSpan.BaggageItem(RootSpanNameKey) == "" {
    #         asyncSpan.SetBaggageItem(RootSpanNameKey, taskName)
    #     }
    #
    #     EnrichSpan(newCtx, keyAndValues...)
    #     return asyncSpan, newCtx
    # }


def create_child_span(task_name, ignore_active_span=False, **kwargs):
    """
    Creates a Child Span from Parent Span embedded in current context context. Should be used before
    starting a new major operation in Synchronous or Asynchronous mode (go routine), such as following:

        1. Start of all implemented External API methods unless using a interceptor for auto-injection
           of Span (Server side impl)
        2. Just before calling an Third-Party lib which is invoking a External API (etcd, kafka)
        3. In start of a Go Routine responsible for performing a major task involving significant duration
        4. Any method which is suspected to be time consuming...
    """
    tracer = global_tracer()
    # if tracer is None or not (GlobalTracingSupport.log_correlation_status or
    #                           GlobalTracingSupport.trace_publishing_status):
    #     # return nop tracer
    #     return Tracer().start_span(task_name)

    parent_span = tracer.active_span
    child_span = tracer.start_span(task_name, child_of=parent_span, ignore_active_span=ignore_active_span)
    #     parentSpan := opentracing.SpanFromContext(ctx)
    #     childSpan, newCtx := opentracing.StartSpanFromContext(ctx, taskName)

    if parent_span is None or not parent_span.get_baggage_item(ROOT_SPAN_KEY_NAME):
        child_span.set_baggage_item(ROOT_SPAN_KEY_NAME, task_name)

    enrich_span(child_span, **kwargs)
    return child_span


def enrich_span(span, **kwargs):
    """ Method to inject additional log fields into Span e.g. device-id """

    if isinstance(span, JaegerSpan):
        # Inject as a BaggageItem when the Span is the Root Span so that it propagates
        # across the components along with Root Span (called as Trace)
        # else, inject as a Tag so that it is attached to the Child Task
        is_root_span = span.trace_id == span.span_id

        for _, field in kwargs.items():
            for key, value in field.items():
                if is_root_span:
                    span.set_baggage_item(key, str(value))
                else:
                    span.set_tag(key, value)


def span_from_context(ctx):
    """ Twisted python equivalent of golang opentracing.SpanFromContext(ctx) """

    if not isinstance(ctx, JaegerSpanContext):
        return None

    # Currently only support if in main thread.
    if not threading.main_thread():
        return None

    # TODO: Do rest of it here
    return None


def extract_context_attributes():
    """
    Extracts details of current context as log fields from the Tracing Span
    injected into the context instance. Following log fields are extracted:

        -  Operation Name : key as 'op-name' and value as Span operation name
        -  Operation Id : key as 'op-id' and value as 64 bit Span Id in hex digits string

    Additionally, any tags present in Span are also extracted to use as log
    fields e.g. device-id.

    If no Span is found associated with context, blank slice is returned without
    any log fields

    :return: (dict) of fields or None
    """
    if not GlobalTracingSupport.log_correlation_status:
        return None

    span = global_tracer().active_span
    if not isinstance(span, JaegerSpan):
        return None

    op_id = span.trace_id & 0xFFFFFFFFFFFFFFFF   # Lower 64-bits only
    op_name = span.get_baggage_item(ROOT_SPAN_KEY_NAME)
    task_id = span.span_id
    task_name = span.operation_name

    if not op_name:
        span.set_baggage_item(ROOT_SPAN_KEY_NAME, task_name)
        op_name = task_name

    fields = {'op-id': '{:016x}'.format(op_id), 'op-name': op_name}

    if task_id != op_id:
        fields['task-id'] = '{:016x}'.format(task_id)
        fields['task-name'] = task_name

    try:
        for key, value in span.tags:
            # Ignore the special tags added by Jaeger, middleware (sampler.type, span.*)
            # present in the span
            if any(key.startswith(prefix) for prefix in ('sampler.', 'span.', 'component.')):
                continue

            fields[key] = value

    except Exception as _e:
        pass

    try:
        for key, value in span.context.baggage.items():
            if key != 'rpc-span-name':
                fields[key] = value

    except Exception as _e:
        pass

    return fields


def mark_span_error(error=None):
    """ Inject error information into the currently active span """

    span = global_tracer().active_span
    if span is not None:
        span.set_tag(tags.ERROR, True)

        if error is not None:
            span.set_tag('err', error)


def traced_function(func=None, name=None, on_start=None, ignore_active_span=False,
                    require_active_trace=False, async_result=False):
    """
    A decorator that enables tracing of the wrapped function or
    twisted deferred routine provided there is a parent span already established.
    .. code-block:: python
        @traced_function
        def my_function1(arg1, arg2=None)
            ...
    :param func: decorated function or Twisted deferred routine

    :param name: optional name to use as the Span.operation_name.
                 If not provided, func.__name__ will be used.

    :param on_start: an optional callback to be executed once the child span
                     is started, but before the decorated function is called. It can be
                     used to set any additional tags on the span, perhaps by inspecting
                     the decorated function arguments. The callback must have a signature
                     `(span, *args, *kwargs)`, where the last two collections are the
                     arguments passed to the actual decorated function.
                     .. code-block:: python
                         def extract_call_site_tag(span, *args, *kwargs)
                             if 'call_site_tag' in kwargs:
                                 span.set_tag('call_site_tag', kwargs['call_site_tag'])
                         @traced_function(on_start=extract_call_site_tag)
                         @inlineCallback             TODO: InlineCallback not yet supported fully/tested
                         def my_function(arg1, arg2=None, call_site_tag=None)
                ...
    :param require_active_trace: controls what to do when there is no active
                                 trace. If require_active_trace=True, then no span is created.
                                 If require_active_trace=False, a new trace is started.

    :param async_result: if a parent trace is present, this specifies that the
                         span will (may) finish asynchronously (possibly after) the parent span
                         and should be treated as more as 'follows-from'

    :param ignore_active_span: if true, start a new root span, ignoring any currently active one

    :return: returns a tracing decorator
    """
    if func is None:
        return functools.partial(traced_function, name=name, on_start=on_start,
                                 ignore_active_span=ignore_active_span,
                                 require_active_trace=require_active_trace,
                                 async_result=async_result)

    operation_name = name if name else func.__name__

    @functools.wraps(func)
    def decorator(*args, **kwargs):
        parent_span = global_tracer().active_span if not ignore_active_span else None
        if parent_span is None and require_active_trace:
            return func(*args, **kwargs)

        tracer = global_tracer()
        if async_result:
            reference_span, parent_span = parent_span, None
        else:
            reference_span = None

        with tracer.start_active_span(operation_name, child_of=parent_span,
                                      references=reference_span, ignore_active_span=ignore_active_span,
                                      finish_on_close=False) as scope:
            span = scope.span

            if callable(on_start):
                on_start(span, *args, **kwargs)

            # We explicitly invoke deactivation callback for the StackContext,
            # because there are scenarios when it gets retained forever, for
            # example when a Periodic Callback is scheduled lazily while in the
            # scope of a tracing StackContext.
            try:
                d = func(*args, **kwargs)

                # Twisted routines and inlineCallbacks generate defers
                if isinstance(d, defer.Deferred):
                    def done(results):
                        span.finish()
                        return results

                    def failed(reason):
                        # Set error in span
                        span.log(event='exception', payload=reason)
                        span.set_tag('error', 'true')
                        span.finish()
                        return reason

                    if d.called:
                        span.finish()
                    else:
                        d.addCallbacks(done, failed)
                else:
                    span.finish()
                return d

            except Exception as e:
                span.set_tag(tags.ERROR, 'true')
                span.log(event='exception', payload=e)
                span.finish()
                raise

    return decorator