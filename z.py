import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor
import time
from mypy.options import Options
from mypy.find_sources import create_source_list

def run_mypy(paths):
    """Run mypy on a list of directory paths."""
    print(len(paths))
    #result = subprocess.run(["mypy", "--cache-dir", "/dev/null", "--explicit-package-bases"] + paths, capture_output=True, text=True, check=False)
    #result = subprocess.run(["mypy", "--cache-dir", "/Users/walmy/dev/cache", "--explicit-package-bases"] + paths, capture_output=True, text=True, check=False)
    result = subprocess.run(["mypy", "--sqlite-cache", "--cache-dir", "/Users/walmy/dev/cache", "--explicit-package-bases"] + paths, capture_output=True, text=True, check=False)
    #result = subprocess.run(["mypy", "--cache-dir", "/Users/walmy/dev/cache", "--sqlite-cache", "--explicit-package-bases", "--skip-cache-mtime-checks"] + paths, capture_output=True, text=True, check=False)
    #if (result.cod)
    #print(result.stdout + result.stderr)
    print(time.time())
    return result.stdout + result.stderr

def chunk_array(array, n):
    """Split an array into n chunks with as even sizes as possible."""
    # shuffle array
    import random
    b = array.copy()
    random.shuffle(b)
    k, m = divmod(len(b), n)
    return [b[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def main(paths, num_threads=1):
        # Use ThreadPoolExecutor to run mypy in parallel
    print(time.time())
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = executor.map(run_mypy, chunk_array(paths, num_threads))

    print(len(list(results)))


if __name__ == "__main__":
    path_dist = []
    for i in range(1, len(sys.argv)):
        sources = create_source_list([sys.argv[i]], Options())
        source_paths = [source.path for source in sources]
        path_dist.extend(source_paths)
    main(path_dist)

