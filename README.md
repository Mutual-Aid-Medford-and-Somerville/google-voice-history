# Google Voice History

Generate a CSV of call and message history from a Google Voice Takeout.

## Developing

Set up your development environment:

- [Fork and clone](https://help.github.com/en/articles/fork-a-repo) this repository

- Set up a [virtual environment](https://docs.python.org/3/tutorial/venv.html):

    ```
    $ python3 -m venv venv

    $ source venv/bin/activate

    $ python -m pip install -U setuptools pip wheel

    $ python -m pip install -r requirements.txt
    ```

    This will install:

    - [black](https://black.readthedocs.io/en/stable/) and [isort](https://pycqa.github.io/isort/) to format the code
    - [flake8](http://flake8.pycqa.org/en/latest/) to check code style (aka "linting")
    - [mypy](https://mypy.readthedocs.io/en/latest/) to check types
    - [pytest](https://docs.pytest.org/en/latest/) to run the tests

Run the tests:

```
$ pytest
```

Format the code and run the checks:

```
$ isort src tests

$ black src tests

$ flake8 src tests

$ mypy src
```
