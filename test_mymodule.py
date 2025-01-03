# This code is part of the IBM Data Engineering Professional Certificate course on Coursera.

import unittest
from mymodule import square, double


class TestSquare(unittest.TestCase):
    """Test cases for the `square` function."""

    def test_square(self):
        """Test square function with various inputs."""
        self.assertEqual(square(2), 4)
        self.assertEqual(square(3.0), 9.0)
        self.assertEqual(square(-3), 9)


class TestDouble(unittest.TestCase):
    """Test cases for the `double` function."""

    def test_double(self):
        """Test double function with various inputs."""
        self.assertEqual(double(2), 4)
        self.assertEqual(double(-3.1), -6.2)
        self.assertEqual(double(0), 0)


if __name__ == "__main__":
    unittest.main()
