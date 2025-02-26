import sys
import pytest
from pathlib import Path


def main():
    """Run the database test suite."""
    # Find the parent directory of this file (tests directory)
    tests_dir = Path(__file__).parent
    
    # Add any default pytest arguments
    args = [str(tests_dir)]
    
    # Pass any command line arguments to pytest
    if len(sys.argv) > 1:
        args.extend(sys.argv[1:])
    
    # Run pytest with the specified arguments
    sys.exit(pytest.main(args))


if __name__ == "__main__":
    main()