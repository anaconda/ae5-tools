## Development Requirements

* Python >=3.8
* conda
* anaconda-project

## Development Environment Setup

> anaconda-project prepare --env-spec default

## Anaconda Project Development Commands

These commands are used during development for solution management.

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

## Contributing

1. Fork the repository on GitHub
2. Create a named feature branch (like `add_component_x`)
3. Write your change
4. Write tests for your change (if applicable)
5. Run the tests, ensuring they all pass
6. Submit a Pull Request using GitHub
