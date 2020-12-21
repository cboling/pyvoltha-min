==================
 ``PyVOLTHA-min``
==================

.. image:: https://img.shields.io/pypi/v/pyvoltha-min.svg
    :target: https://pypi.python.org/pypi/pyvoltha-min/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/pyvoltha-min.svg
        :target: https://pypi.org/project/pyvoltha-min/
        :alt: Supported Python versions

PyVOLTHA-min is an updated pyVOLTHA package that provides a collection
of python 3.x libraries used to create an VOLTHA OLT device adapter
focused on the VOLTHA v2.4 release and beyond.

As some adapters (including the OpenONU) have required to be moved to
the Go language, there is was not as much maintenance performed on the
pyVOLTHA package.

The goal for this package is to begin to deprecate and remove old
VOLTHA 1.x features and focus on a minimal set of updated imports that can
be supported in Python 3.6+ with the hopes to transition to later versions
of python. In addition, effort to remove any GPL or other restrictive
package requirements is also highly desired.

Installation instruction
------------------------

.. code:: bash

   pip install pyvoltha-min

Release Notes
-------------
v2.5.1 (2020-12-21)
^^^^^^^^^^^^^^^^^^^

 - Support for force-delete adapter
 - Added jaeger-client for log correlation and span support
 - Added get_ext_value() IAdapter interface

v2.5.0 (2020-12-21)
^^^^^^^^^^^^^^^^^^^

 - Upgraded Voltha-protos requirements to 4.0.5 tag. This is the VOLTHA v2.5 tag
   plus 3 additional protobuf changes from VOLTHA v2.6 that will have little impact
   until additional capabilities are added to device adapters that need them.

v2.4.7 (2020-12-04)
^^^^^^^^^^^^^^^^^^^

 - Endpoint Manager work to properly read out instances when adapter scaling had been performed
 - Extended interadapter IAdapter interface to pass along the 'from-adapter' topic to allow for
   auto-learning of ONU device adapter endpoint.

v2.4.6 (2020-12-03)
^^^^^^^^^^^^^^^^^^^

 - Added Rx/Tx frame-size (buckets) counters to NNI and PON statistics
 - Deprecated IndexPool, IdGeneration, docker_helpers, MessageQueue, and a majority
   of the common.config files (only EtcdStore in config_backend.py is still in use)
   EtcdStore will be deprecated in the near future and replaced with the async version
   (TwistedEtcdStore)

v2.4.5 (2020-12-01)
^^^^^^^^^^^^^^^^^^^

 - Require kv_store instance during instantiation of a onu single-instance tech profiler
 - Config backend list() method should return the generator that etcd returned

v2.4.5 (2020-11-25)
^^^^^^^^^^^^^^^^^^^

 - More work to on twisted TimeoutError. It actually is defined as a class with the
   same name in more than one module.

v2.4.4 (2020-11-23)
^^^^^^^^^^^^^^^^^^^

 - Use Twisted TimeoutError exception rather than defining own Exception class
 - Improved timeout handling/error checking of inter-adapter exceptions to minimize
   additional exceptions being thrown by twisted reactor while in an inlineCallback
 - Work to support base python version of 3.8.5+.  Needs more work in pyYAML and
   the confluent-kafka modules to support 3.8 of python
 - Cleanup of remaining warnings (all low) identified by bandit
 - Move Development Status classifier to level 5 - Production/Stable
 - Dropped simplejson and docker-py packages as they are not needed

v2.4.3 (2020-11-19)
^^^^^^^^^^^^^^^^^^^

 - Added some reasonable max/min values on the PM Config frequency & skew
 - ONU and GEM Port stats are optional and not configured by default to match what
   the OpenOLT currently supports

v2.4.2 (2020-11-18)
^^^^^^^^^^^^^^^^^^^

 - Updated requirements (most notably confluent-kafka) to latest versions
 - Kafka requests now run in their own tasks
 - Support alarm (ONU Signal Fail) if deregistation due to degraded signal occurs

v2.4.1 (2020-11-16)
^^^^^^^^^^^^^^^^^^^

 - Updated requirements (most notably txaioetcd) to latest versions
 - Provide optional etcd change watch callback to be specified by external user
 - Allow watch callback to work for a prefix (more efficient with logger callbacks)

v2.4.0 (2020-10-29)
^^^^^^^^^^^^^^^^^^^

 - Initial v2.4 release

v2.0.9 (2020-10-28)
^^^^^^^^^^^^^^^^^^^

- Lowered log message level for twisted-etcd-store success calls.

v2.0.8 (2020-10-22)
^^^^^^^^^^^^^^^^^^^

- For async/twisted ETCD client, differentiate between a cancelled async request and true failure
- Start method for PM metrics will check to for an existing running LoopingCall before attempting
  to start the loop (which would assert otherwise if already running)

v2.0.7 (2020-10-13)
^^^^^^^^^^^^^^^^^^^

- Added support for Device Event serialization to support HA reconciliation after
  a container restart

v2.0.6 (2020-10-12)
^^^^^^^^^^^^^^^^^^^

- Check to not stop looping call in stats if not running. Prevents an assert
- EtcStore errback should return the reason, not raise an assert
- Additional work on logger level and components in preparation for v2.5+ support
- Update to reported KPI Metrics to better match what OpenOLT supports in v2.4
- Allow None to be passed as key to TwistedEtcdStore operations to select the base client path
  and allow a timeout when initializing the etcd client.

v2.0.5 (2020-10-06)
^^^^^^^^^^^^^^^^^^^

- Fix bad check on OperStatus type. Always passed in as an int
- Fix log keyword bug, should not use 'event' in call
- Disable GEM Port statistics until we are ready for them

v2.0.4 (2020-10-05)
^^^^^^^^^^^^^^^^^^^

- Deprecation of HeartBeat Event, now called OLT Indication
- Correct subcategory for OLT LOS Event (was ONU, should be OLT)
- Corrected Device Events for OLT LOS, OLT Port Down, OLT Down, Dying
  Gasp, and PON Interface Down events for the OLT (VOLTHA v2.x format)
- A small amount of pylint cleanup and python 3 updates
- Call to etcd callback needs to be placed onto reactor thread

v2.0.3 (2020-09-30)
^^^^^^^^^^^^^^^^^^^

- Call to etcd callback needs to be placed onto reactor thread

v2.0.2 (2020-09-28)
^^^^^^^^^^^^^^^^^^^

-  Default KPI subcategory is now OLT and can be set with a kwargs if needed
   for some other type
-  Moved to latest version of protobuf module
-  Added golang-equivalent Endpoint Manager in effort to determine endpoint
   of a device for interadapter-messages.  Turns out there is a flaw in the
   design and is reliant upon use of a specific golang 3rd party hashing
   algorythm which may not be available to a python program.  Discussions
   on the VOLTHA slack channel have been started and a JIRA may be issued
   in the near future.
-  Added 'list' function for ectd library

v2.0.1 (2020-09-24)
^^^^^^^^^^^^^^^^^^^

-  Move etcd/kafka address values to be similar to what OpenOLT uses
-  Small amount of 'assert' cleanup flagged by bandit


v2.0.0 (2020-09-20)
^^^^^^^^^^^^^^^^^^^

-  Pre-release with all but Alarms/Events and logging up to date
   with v2.4 release of VOLTHA
-  Much refactoring of python 2.7 code with movement toward at
   least python 3.5 and later supported
-  Dropped import of __future__ and six (to some extent)
-  Removed simple ONU-only device events related to OMCI
-  Dropped transitions, pcapy, and scapy imports (no longer required)
-  Added missing 'child_device_lost' IAdapter RPC as well as
   a few other IAdapter and inter-adapter API bit rot cleaned up

