.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/tmolteno/dask-ms/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

dask-ms could always use more documentation, whether as part of the
official dask-ms docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/tmolteno/dask-ms/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `daskms` for local development.

1. Fork the `daskms` repo on GitHub.
2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/daskms.git

3. Install your local copy into a virtualenv. Assuming you have virtualenvwrapper installed, this is how you set up your fork for local development::

    $ mkvirtualenv daskms
    $ cd daskms/
    $ python setup.py develop

4. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass flake8 and the
   tests, including testing other Python versions with tox::

    $ flake8 daskms tests
    $ python setup.py test or py.test
    $ tox

   To get flake8 and tox, just pip install them into your virtualenv.

6. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

7. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for the Python versions listed in
   ``pyproject.toml``. Check the GitHub Actions CI run on the pull request
   and make sure that the tests pass for all supported Python versions.

Tips
----

To run a subset of tests::

$ py.test -vvv daskms/tests


Deploying
---------

A reminder for the maintainers on how to deploy.

A release must keep every version marker coherent:

* ``pyproject.toml`` ``version``
* ``daskms/__init__.py`` ``__version__`` (also feeds ``docs/conf.py``)
* ``tbump.toml`` ``current``
* ``uv.lock`` (root ``dask-ms-2`` package version)
* ``HISTORY.rst`` top section
* the annotated git tag ``vZ.Y.Z`` -- the ``Publish dask-ms-2 package``
  workflow only triggers on ``v*.*.*`` tags

1. Add the intended release number to ``HISTORY.rst`` as ``Z.Y.Z
   (unreleased)`` and commit it.

2. Run the release task. It rewrites every version marker, dates the
   ``HISTORY.rst`` section, commits ``Release Z.Y.Z: ...``, creates the
   annotated tag ``vZ.Y.Z`` (message ``dask-ms-2 Z.Y.Z``) and pushes the
   branch and the tag together with ``git push --atomic``::

       $ invoke release Z.Y.Z --reason "what changed"

   Variants::

       $ invoke release Z.Y.Z --reason "..." --dry-run   # print the plan only
       $ invoke release Z.Y.Z --reason "..." --no-push   # local commit + tag only

   The marker audit runs on demand::

       $ invoke versions

3. Pushing the ``v*.*.*`` tag triggers the ``Publish dask-ms-2 package``
   GitHub Actions workflow, which builds the wheel and source distribution
   and publishes them to PyPI with Trusted Publishing.
