# Project setup

This project is made with uv, use uv run to run stuff inside of it

# Coding style

Use typehints when possible
Avoid to many try catch blogs.
Use eliot actions for logging.
We prefer polars over pandas.
Avoid try catch when possible, we have logging to log errors.
If Pydantic is used assume Pydantic >= 2. Also, if the class is derivative of the pydantic model do no generate __init__ method.

# Testing style

Avoid mocking, when generating tests prefer generating proper integration tests with real data