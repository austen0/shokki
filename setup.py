import setuptools

setuptools.setup(
  name="shokki",
  version="0.0.1",
  author="austen0",
  author_email="austen@cas3y.com",
  description=("shokki distributes repeated function calls across system "
               "resources."),
  #long_description=long_description,
  #long_description_content_type="text/markdown",
  url="https://github.com/austen0/shokki",
  packages=setuptools.find_packages(exclude=["tests"]),
)
