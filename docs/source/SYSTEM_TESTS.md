# System Tests

## Overview
AE5 system and load testing can be accomplished using the ae5-tools test harness.

## Setup
The test harness can generate (but will not forcibly recreate by default), and tear-down (if configured to do so) all required fixtures on the target system. To accomplish this, the harness will leverage the realm administrator account for instance.

The configuration is driven by environmental variables. The defaults managed for the harness are as follows:

| Variable           | Default              | Description                      |
|--------------------|----------------------|----------------------------------|
| AE5_HOSTNAME       | anaconda.example.com | AE5 instance F.Q.D.N.            |
| AE5_ADMIN_USERNAME | admin                | AE5 realm admin username         |
| AE5_ADMIN_PASSWORD | admin                | AE5 realm admin password         |
| AE5_K8S_ENDPOINT   | ssh:centos           | K8S service endpoint declaration |
| AE5_K8S_PORT       | 23456                | K8S service port                 |
| CI                 | False                | CI environment test skip flag    |


All harness environmental variables can be over-ridden or extended.

The defaults will be over-ridden if the environmental variables are defined before the harness is executed. (e.g. the harness will not over-ride them if they already exist).  This allows for flexibility for scenarios such as build-runners, local development, or remote testing against an instance.
As-is the tests can run out-of-the-box against a local development instance.

The defaults are defined for the system and load tests within `tests/system/.env` and `tests/load/.env` respectively.

## Fixture Control

### System Tests
The fixtures and flags for the creation and removal are defined within the configuration here: `tests/fixtures/system/fixtures.json`.

The harness currently supports the creation of the following fixture types:
1. User accounts
2. Projects (from upload)

Additional fixture support can be added by extending the fixture manager.

Test Suites are subclasses of a fixture manager and include logic for the configuration of the fixtures and their relationships specific to the test suite in execution.

**Default Fixtures and Control Flags**
```json lines
{
  "force": false,
  "teardown": false,
  "accounts": [
    {
      "id": "1",
      "username": "tooltest",
      "email": "tooltest@localhost.local",
      "firstname": "tooltest",
      "lastname": "1",
      "password": "tooltest"
    },
    {
      "id": "2",
      "username": "tooltest2",
      "email": "tooltest2@localhost.local",
      "firstname": "tooltest",
      "lastname": "2",
      "password": "tooltest2"
    },
    {
      "id": "3",
      "username": "tooltest3",
      "email": "tooltest3@localhost.local",
      "firstname": "tooltest",
      "lastname": "3",
      "password": "tooltest3"
    }
  ],
  "projects": [
    {
      "name": "testproj1",
      "artifact": "tests/fixtures/system/testproj1.tar.gz",
      "tag": "0.1.0"
    },
    {
      "name": "testproj2",
      "artifact": "tests/fixtures/system/testproj2.tar.gz",
      "tag": "0.1.0"
    },
    {
      "name": "testproj3",
      "artifact": "tests/fixtures/system/testproj3.tar.gz",
      "tag": "0.1.0"
    }
  ]
}
```

## Execution
The test suites can be run by executing either of the below commands.

**System Tests**

```commandline
anaconda-project run test:system
```

**Load Tests**

**WARNING** - Load testing should **NEVER** be done again a production instance - it **CAN** bring down the instance if too much load is requested.

```commandline
anaconda-project run test:load
```
