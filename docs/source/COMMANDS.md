# Project Commands

Project Commands

### Development

These commands are used during project development.

| Command          | Environment | Description                               |
|------------------|-------------|:------------------------------------------|
| bash             | Default     | Run bash within the `default` environment |
| clean            | Default     | Cleanup temporary project files           |
| lint             | Default     | Perform code linting check                |
| lint:fix         | Default     | Perform automated code formatting         |
| test             | Default     | Run all test suites                       |
| test:unit        | Default     | Unit Test Suite                           |
| test:integration | Default     | Integration Test Suite                    |
| test:system      | Default     | System Test Suite                         |


### Runtime

These commands are used to execute the different components of the project.

| Command             | Environment | Description                                                                                                   |
|---------------------|-------------|:--------------------------------------------------------------------------------------------------------------|
| (default)           | k8s_server  | Launch the K8S service integration component.                                                                 |
