#
# Copyright 2020 the original author or authors.
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
#
import os
import structlog
from opentracing import global_tracer

log = structlog.get_logger()


class _LogFeatures:
    def __init__(self):
        self._log_correlation_enabled = None
        self._trace_publishing_enabled = None
        self._component_name = None
        self._active_trace_agent_address = None
        self._scope_manager = None
        self._log_controller = None

    @property
    def log_correlation_status(self):
        return self._log_correlation_enabled is not None and self._log_correlation_enabled

    @log_correlation_status.setter
    def log_correlation_status(self, value):
        if not isinstance(value, bool):
            raise TypeError('Boolean required')

        if value != self._log_correlation_enabled:
            # Close any previous tracer and reopen if required
            tracer = global_tracer()
            if hasattr(tracer, 'close'):
                tracer.close()

            self._log_correlation_enabled = value
            self._log_controller.enable_jaeger_client()

    @property
    def trace_publishing_status(self):
        return self._trace_publishing_enabled is not None and self._trace_publishing_enabled

    @trace_publishing_status.setter
    def trace_publishing_status(self, value):
        if not isinstance(value, bool):
            raise TypeError('Boolean required')

        if value != self._trace_publishing_enabled:
            # Close any previous tracer and reopen if required
            tracer = global_tracer()
            if hasattr(tracer, 'close'):
                tracer.close()

            self._trace_publishing_enabled = value
            self._log_controller.enable_jaeger_client()

    @property
    def component_name(self):
        return self._component_name

    @property
    def active_trace_agent_address(self):
        return self._active_trace_agent_address

    @property
    def scope_manager(self):
        return self._scope_manager

    @scope_manager.setter
    def scope_manager(self, value):
        if self._scope_manager is None:
            self._scope_manager = value

    def set_log_controller(self, log_controller, scope_manager, config):
        self._log_controller = log_controller
        self._active_trace_agent_address = config.get('trace_agent_address')
        self._component_name = config.get('component_name', os.environ.get('COMPONENT_NAME',
                                                                           'unknown-olt'))
        self.scope_manager = scope_manager
        # Set trace/log correlation directly.  During initial startup, we want to start the
        # OpenTracing client at the end of our init and not from the property setter functions.
        self._trace_publishing_enabled = config.get('trace_enabled')
        self._log_correlation_enabled = config.get('log_correlation_enabled')


GlobalTracingSupport = _LogFeatures()
