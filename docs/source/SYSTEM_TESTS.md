# Test Systems

## Overview
AE5 system and load testing can be accomplished using the ae5-tools system test runners.

## Setup
The test harness will generate and tear-down all required fixtures on the target system. To accomplish this, the harness will leverage the realm administrator account for the instance.

The configuration is driven by environmental variables. The defaults for the runner are as follows:

> AE5_HOSTNAME=anaconda.example.com
>
> AE5_ADMIN_USERNAME=admin
> 
> AE5_ADMIN_PASSWORD=admin

The defaults will be over-ridden if the above environmental variables are defined before the harness is executed.  As-is the tests can run out-of-the-box against a local development instance.

## Fixture Control

### System Tests
The fixtures and flags for the creation and removal are defined within the configuration here: `tests/fixtures/system/fixtures.json`.

By default, fixtures will not be forcibly recreated or destroyed.



## Execution
Execution of the test suites can be accomplished by executing either of the below commands.

For system testing:

```commandline
anaconda-project run test:system
```

For load testing:

**WARNING**

* Load testing should **NEVER** be done again a production instance - it **CAN** bring down the instance.

```commandline
anaconda-project run test:load
```
