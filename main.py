#!/usr/bin/env python3
"""
Main entry point for the info-to-nifi project.
Discovers and executes all pipeline modules in the src directory.
"""

import os
import sys
import importlib.util
import logging
from pathlib import Path
from typing import List, Dict, Any
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class PipelineRunner:
    """Discovers and runs all pipeline modules."""
    
    def __init__(self, src_dir: str = "src"):
        self.src_dir = Path(src_dir)
        self.modules: List[Dict[str, Any]] = []
        
    def discover_modules(self) -> None:
        """Discover all Python modules in the src directory."""
        if not self.src_dir.exists():
            logger.error(f"Source directory {self.src_dir} does not exist")
            return
            
        logger.info(f"Discovering modules in {self.src_dir}")
        
        for py_file in self.src_dir.rglob("*.py"):
            if py_file.name.startswith("__"):
                continue
                
            module_name = py_file.stem
            relative_path = py_file.relative_to(self.src_dir.parent)
            
            self.modules.append({
                "name": module_name,
                "path": py_file,
                "relative_path": relative_path
            })
            
        logger.info(f"Discovered {len(self.modules)} modules")
        
    def load_module(self, module_info: Dict[str, Any]) -> Any:
        """Load a Python module dynamically."""
        try:
            spec = importlib.util.spec_from_file_location(
                module_info["name"],
                module_info["path"]
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_info["name"]] = module
                spec.loader.exec_module(module)
                return module
        except Exception as e:
            logger.error(f"Failed to load module {module_info['name']}: {e}")
            logger.debug(traceback.format_exc())
        return None
        
    def execute_module(self, module: Any, module_name: str) -> bool:
        """Execute a module's main function or class."""
        try:
            # Try to find and execute main function
            if hasattr(module, "main"):
                logger.info(f"Executing main() in {module_name}")
                result = module.main()
                logger.info(f"Module {module_name} completed with result: {result}")
                return True
                
            # Try to find and execute run function
            elif hasattr(module, "run"):
                logger.info(f"Executing run() in {module_name}")
                result = module.run()
                logger.info(f"Module {module_name} completed with result: {result}")
                return True
                
            # Try to find and instantiate a processor class
            elif hasattr(module, "Processor"):
                logger.info(f"Instantiating Processor class in {module_name}")
                processor = module.Processor()
                if hasattr(processor, "process"):
                    result = processor.process()
                    logger.info(f"Module {module_name} completed with result: {result}")
                    return True
                    
            # Try to execute extract, transform, load pattern
            elif hasattr(module, "extract") or hasattr(module, "transform") or hasattr(module, "load"):
                logger.info(f"Executing ETL pattern in {module_name}")
                
                data = None
                if hasattr(module, "extract"):
                    logger.info(f"Running extract in {module_name}")
                    data = module.extract()
                    
                if hasattr(module, "transform") and data is not None:
                    logger.info(f"Running transform in {module_name}")
                    data = module.transform(data)
                    
                if hasattr(module, "load") and data is not None:
                    logger.info(f"Running load in {module_name}")
                    module.load(data)
                    
                logger.info(f"Module {module_name} ETL completed")
                return True
                
            else:
                logger.warning(f"No executable entry point found in {module_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error executing module {module_name}: {e}")
            logger.debug(traceback.format_exc())
            return False
            
    def run(self) -> None:
        """Run all discovered modules."""
        logger.info("=" * 80)
        logger.info("Starting info-to-nifi Pipeline Runner")
        logger.info("=" * 80)
        
        self.discover_modules()
        
        if not self.modules:
            logger.warning("No modules found to execute")
            return
            
        # Sort modules to ensure framework modules run first
        framework_modules = [m for m in self.modules if "framework" in str(m["path"])]
        other_modules = [m for m in self.modules if "framework" not in str(m["path"])]
        
        execution_order = framework_modules + other_modules
        
        success_count = 0
        failure_count = 0
        skipped_count = 0
        
        for module_info in execution_order:
            logger.info("-" * 80)
            logger.info(f"Processing: {module_info['relative_path']}")
            
            module = self.load_module(module_info)
            if module is None:
                failure_count += 1
                continue
                
            executed = self.execute_module(module, module_info["name"])
            if executed:
                success_count += 1
            else:
                skipped_count += 1
                
        logger.info("=" * 80)
        logger.info("Pipeline Execution Summary")
        logger.info(f"Total modules: {len(self.modules)}")
        logger.info(f"Successfully executed: {success_count}")
        logger.info(f"Failed: {failure_count}")
        logger.info(f"Skipped: {skipped_count}")
        logger.info("=" * 80)


def check_environment() -> bool:
    """Check if the required environment is ready."""
    logger.info("Checking environment...")
    
    # Check if config.yaml exists
    if not Path("config.yaml").exists():
        logger.warning("config.yaml not found, creating default configuration")
        create_default_config()
        
    # Check if data directory exists
    data_dir = Path("data")
    if not data_dir.exists():
        logger.info("Creating data directory")
        data_dir.mkdir(parents=True, exist_ok=True)
        
    # Check if templates directory exists
    templates_dir = Path("templates")
    if not templates_dir.exists():
        logger.info("Creating templates directory")
        templates_dir.mkdir(parents=True, exist_ok=True)
        
    return True


def create_default_config() -> None:
    """Create a default config.yaml file."""
    default_config = """
# Apache NiFi Configuration
nifi:
  host: localhost
  port: 8080
  username: admin
  password: ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB
  use_ssl: false

# Database Configuration
database:
  host: localhost
  port: 5432
  name: nifi_db
  user: nifi_user
  password: nifi_password

# Redis Configuration
redis:
  host: localhost
  port: 6379
  db: 0

# Logging Configuration
logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Pipeline Configuration
pipeline:
  batch_size: 1000
  retry_attempts: 3
  timeout: 300
"""
    
    with open("config.yaml", "w") as f:
        f.write(default_config)
    logger.info("Created default config.yaml")


def main():
    """Main entry point."""
    try:
        logger.info("info-to-nifi Pipeline Starting...")
        
        # Check environment
        if not check_environment():
            logger.error("Environment check failed")
            sys.exit(1)
            
        # Run pipeline
        runner = PipelineRunner()
        runner.run()
        
        logger.info("Pipeline execution completed")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()