# Contributing

Thank you for your interest in improving **FHIR OMOP ETL**.  This guide outlines
how to contribute code, documentation, or bug reports.

## Coding standards
- Follow [PEP 8](https://peps.python.org/pep-0008/) for style.
- Use type hints for all public functions and methods.
- Write docstrings using the [Google style](https://google.github.io/styleguide/pyguide.html#s3.8-comments-and-docstrings).
- Keep functions small and focused; prefer composition over large monolithic
  functions.

## Commit messages
- Use the present tense and imperative mood: `add feature` not `added`.
- Limit the first line to 50 characters; wrap additional details at 72
  characters.
- Reference related issues with `Fixes #123` or `Refs #123` when applicable.

## Pull request process
1. Create a new branch for your work; do not commit to `main`.
2. Add or update tests when you change behavior or fix bugs.
3. Run the test suite locally with `pytest -q` and ensure it passes.
4. Open a pull request and request review.  Describe your changes and any
   potential impacts.
5. A project maintainer will review your contribution.  Be responsive to
   feedback and make updates as needed.

## Sample workflow
1. Fork the repository on GitHub.
2. Create a feature branch: `git checkout -b feature/my-feature`.
3. Implement your change and add tests.
4. Use files in the [`examples/`](examples) directory as input/output samples
   when developing.
5. Run `pytest -q` and commit your work.
6. Push the branch to your fork and open a pull request.

We appreciate your contributions and look forward to your pull requests!
