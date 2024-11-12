import functools
import hashlib
import pickle
from pathlib import Path
import threading
from datetime import datetime
from typing import Callable, Any

# Define a base cache directory
BASE_CACHE_DIR = Path.home() / ".cache" / "awesome_search"


def disk_cache_results(func: Callable) -> Callable:
    """
    A decorator to cache the results of class methods on disk based on their arguments.
    It retains the date of caching and retrieves the latest cache entry when available.
    Adds a 'force' keyword argument to bypass the cache when needed.
    """
    cache_lock = threading.Lock()

    @functools.wraps(func)
    def wrapper(self, *args, force=False, **kwargs) -> Any:
        # Define the cache directory structure: BASE_CACHE_DIR / ClassName / MethodName
        class_name = self.__class__.__name__
        method_name = func.__name__

        cache_base_dir = BASE_CACHE_DIR / class_name / method_name
        cache_base_dir.mkdir(parents=True, exist_ok=True)

        # Serialize the arguments to create a unique cache key
        try:
            key_data = (args, frozenset(kwargs.items()))
            key_bytes = pickle.dumps(key_data)
            key_hash = hashlib.sha256(key_bytes).hexdigest()
        except (pickle.PicklingError, TypeError) as e:
            self.logger.error(f"Failed to create cache key for {func.__name__}: {e}")
            return func(self, *args, **kwargs)

        # Function to find the latest cache file for the given key_hash
        def find_latest_cache_file():
            cache_files = []
            for date_dir in cache_base_dir.iterdir():
                if date_dir.is_dir():
                    cache_file = date_dir / f"{key_hash}.pkl"
                    if cache_file.exists():
                        try:
                            # Parse the date from directory name
                            cache_date = datetime.strptime(date_dir.name, "%Y%m%d")
                            cache_files.append((cache_date, cache_file))
                        except ValueError:
                            self.logger.warning(f"Unexpected directory name format: {date_dir.name}")
            if not cache_files:
                return None
            # Sort cache files by date descending and return the latest one
            cache_files.sort(reverse=True, key=lambda x: x[0])
            return cache_files[0][1]

        cache_file = find_latest_cache_file()

        if force:
            self.logger.debug(f"Force flag is set. Bypassing cache for {func.__name__} with key={key_hash}")
            result = func(self, *args, **kwargs)
            # Save the result to a cache file in today's directory
            today_str = datetime.now().strftime("%Y%m%d")
            today_cache_dir = cache_base_dir / today_str
            today_cache_dir.mkdir(parents=True, exist_ok=True)
            today_cache_file = today_cache_dir / f"{key_hash}.pkl"
            try:
                with cache_lock:
                    with open(today_cache_file, 'wb') as f:
                        pickle.dump(result, f)
                self.logger.debug(f"Result cached to {today_cache_file}")
            except Exception as e:
                self.logger.error(f"Failed to write cache file {today_cache_file}: {e}")
            return result

        if cache_file:
            self.logger.debug(f"Cache hit for {func.__name__} with key={key_hash} from {cache_file.parent.name}")
            try:
                with cache_lock:
                    with open(cache_file, 'rb') as f:
                        result = pickle.load(f)
                self.logger.debug(f"Loaded result from cache file {cache_file}")
                return result
            except Exception as e:
                self.logger.error(f"Failed to read cache file {cache_file}: {e}")
                # Proceed to execute the function if cache reading fails

        self.logger.debug(f"Cache miss for {func.__name__} with key={key_hash}. Executing function.")
        result = func(self, *args, **kwargs)
        # Save the result to a cache file in today's directory
        today_str = datetime.now().strftime("%Y%m%d")
        today_cache_dir = cache_base_dir / today_str
        today_cache_dir.mkdir(parents=True, exist_ok=True)
        today_cache_file = today_cache_dir / f"{key_hash}.pkl"
        try:
            with cache_lock:
                with open(today_cache_file, 'wb') as f:
                    pickle.dump(result, f)
            self.logger.debug(f"Result cached to {today_cache_file}")
        except Exception as e:
            self.logger.error(f"Failed to write cache file {today_cache_file}: {e}")
        return result

    return wrapper
