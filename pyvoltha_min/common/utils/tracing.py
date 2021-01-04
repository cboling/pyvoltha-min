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

import structlog
from opentracing import global_tracer, Format, Tracer, tags
from jaeger_client import Span as JaegerSpan

ROOT_SPAN_KEY_NAME = "op-name"

log = structlog.get_logger()


class _TracingSupport:
    def __init__(self):
        self._log_correlation_enabled = None
        self._trace_publishing_enabled = None
        self._component_name = None
        self._active_trace_agent_address = None

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


GlobalTracingSupport = _TracingSupport()


class NilSpan:
    """ Span that is not an opentrace span """
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        NilSpan._on_error(self, exc_type, exc_val, exc_tb)

    def __str__(self):
        return 'NilSpan'

    @staticmethod
    def _on_error(span, exc_type, exc_val, exc_tb):
        if span and exc_val:
            log.info('exception', message=str(exc_val), kind=exc_type, stack=str(exc_tb))


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


def create_async_span(ctx, span_name):
    return None, None

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


def create_child_span(ctx, task_name, **kwargs):
    """
    Creates a Child Span from Parent Span embedded in passed context. Should be used before starting a new major
    operation in Synchronous or Asynchronous mode (go routine), such as following:

        1. Start of all implemented External API methods unless using a interceptor for auto-injection
           of Span (Server side impl)
        2. Just before calling an Third-Party lib which is invoking a External API (etcd, kafka)
        3. In start of a Go Routine responsible for performing a major task involving significant duration
        4. Any method which is suspected to be time consuming...

    """
    tracer = global_tracer()
    if tracer is None or not (GlobalTracingSupport.log_correlation_status or
                              GlobalTracingSupport.trace_publishing_status):
        # return nop tracer
        return Tracer().start_span(task_name), ctx

    parent_span = tracer.active_span
    child_span = tracer.start_span(task_name, child_of=parent_span)
    #     parentSpan := opentracing.SpanFromContext(ctx)
    #     childSpan, newCtx := opentracing.StartSpanFromContext(ctx, taskName)

    if parent_span is None or not parent_span.get_baggage_item(ROOT_SPAN_KEY_NAME):
        child_span.set_baggage_item(ROOT_SPAN_KEY_NAME, task_name)

    enrich_span(child_span, **kwargs)
    return child_span, ctx
    #     EnrichSpan(newCtx, keyAndValues...)
    #     return childSpan, newCtx
    # }


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

# // Extracts details of Execution Context as log fields from the Tracing Span injected into the
# // context instance. Following log fields are extracted:
# // 1. Operation Name : key as 'op-name' and value as Span operation name
# // 2. Operation Id : key as 'op-id' and value as 64 bit Span Id in hex digits string
# //
# // Additionally, any tags present in Span are also extracted to use as log fields e.g. device-id.
# //
# // If no Span is found associated with context, blank slice is returned without any log fields
# func (lfm *LogFeaturesManager) ExtractContextAttributes(ctx context.Context) []interface{} {
# 	if !lfm.isLogCorrelationEnabled {
# 		return make([]interface{}, 0)
# 	}
#
# 	attrMap := make(map[string]interface{})
#
# 	if ctx != nil {
# 		if span := opentracing.SpanFromContext(ctx); span != nil {
# 			if jspan, ok := span.(*jtracing.Span); ok {
# 				// Add Log fields for operation identified by Root Level Span (Trace)
# 				opId := fmt.Sprintf("%016x", jspan.SpanContext().TraceID().Low) // Using Sprintf to avoid removal of leading 0s
# 				opName := jspan.BaggageItem(RootSpanNameKey)
#
# 				taskId := fmt.Sprintf("%016x", uint64(jspan.SpanContext().SpanID())) // Using Sprintf to avoid removal of leading 0s
# 				taskName := jspan.OperationName()
#
# 				if opName == "" {
# 					span.SetBaggageItem(RootSpanNameKey, taskName)
# 					opName = taskName
# 				}
#
# 				attrMap["op-id"] = opId
# 				attrMap["op-name"] = opName
#
# 				// Add Log fields for task identified by Current Span, if it is different
# 				// than operation
# 				if taskId != opId {
# 					attrMap["task-id"] = taskId
# 					attrMap["task-name"] = taskName
# 				}
#
# 				for k, v := range jspan.Tags() {
# 					// Ignore the special tags added by Jaeger, middleware (sampler.type, span.*) present in the span
# 					if strings.HasPrefix(k, "sampler.") || strings.HasPrefix(k, "span.") || k == "component" {
# 						continue
# 					}
#
# 					attrMap[k] = v
# 				}
#
# 				processBaggageItems := func(k, v string) bool {
# 					if k != "rpc-span-name" {
# 						attrMap[k] = v
# 					}
# 					return true
# 				}
#
# 				jspan.SpanContext().ForeachBaggageItem(processBaggageItems)
# 			}
# 		}
# 	}
#
# 	return serializeMap(attrMap)
# }

def mark_span_error(ctx, error):
    """ Inject error information into the span """
    tracing = global_tracer()
    span = tracing.active_span if tracing is not None else None

    if span is not None:
        span.set_tag(tags.ERROR, True)
        span.set_tag('err', error)

# func MarkSpanError(ctx context.Context, err error) {
# 	span := opentracing.SpanFromContext(ctx)
# 	if span != nil {
# 		span.SetTag("error", true)
# 		span.SetTag("err", err)
# 	}
# }
