import inspect
import typing as t
from dataclasses import dataclass
from pathlib import Path

from dagster import Config
from pydantic import Field
from sqlmesh.core.config import Config as MeshConfig
from sqlmesh.core.config.loader import load_configs

if t.TYPE_CHECKING:
    from dagster_sqlmesh.translator import SQLMeshDagsterTranslator


@dataclass
class ConfigOverride:
    config_as_dict: dict[str, t.Any]

    def dict(self) -> dict[str, t.Any]:
        return self.config_as_dict


class SQLMeshContextConfig(Config):
    """A very basic sqlmesh configuration. Currently you cannot specify the
    sqlmesh configuration entirely from dagster. It is intended that your
    sqlmesh project define all the configuration in it's own directory which
    also ensures that configuration is consistent if running sqlmesh locally vs
    running via dagster.
    
    The config also manages the translator class used for converting SQLMesh
    models to Dagster assets. You can specify a custom translator by setting
    the translator_class_name field to the fully qualified class name.
    """

    path: str
    gateway: str
    config_override: dict[str, t.Any] | None = Field(default_factory=lambda: None)
    translator_class_name: str = Field(
        default="dagster_sqlmesh.translator.SQLMeshDagsterTranslator",
        description="Fully qualified class name of the SQLMesh Dagster translator to use"
    )
    virtual_environment_mode: str | None = Field(default=None)
    environment_catalog_mapping: dict[str, str] | None = Field(default=None)
    default_target_environment: str | None = Field(default=None)
    
    def get_translator(self) -> "SQLMeshDagsterTranslator":
        """Get a translator instance using the configured class name.
        
        Imports and validates the translator class, then creates a new instance.
        The class must inherit from SQLMeshDagsterTranslator.
        
        Returns:
            SQLMeshDagsterTranslator: A new instance of the configured translator class
            
        Raises:
            ValueError: If the imported object is not a class or does not inherit 
                       from SQLMeshDagsterTranslator
        """
        from importlib import import_module
        
        from dagster_sqlmesh.translator import SQLMeshDagsterTranslator
        
        module_name, class_name = self.translator_class_name.rsplit(".", 1)
        module = import_module(module_name)
        translator_class = getattr(module, class_name)
        
        # Validate that the imported class inherits from SQLMeshDagsterTranslator
        if not inspect.isclass(translator_class):
            raise ValueError(
                f"'{self.translator_class_name}' is not a class. "
                f"Expected a class that inherits from SQLMeshDagsterTranslator."
            )
        
        if not issubclass(translator_class, SQLMeshDagsterTranslator):
            raise ValueError(
                f"Translator class '{self.translator_class_name}' must inherit from "
                f"SQLMeshDagsterTranslator. Found class that inherits from: "
                f"{[base.__name__ for base in translator_class.__bases__]}"
            )
        
        return translator_class()

    @property
    def sqlmesh_config(self) -> MeshConfig:
        if self.config_override:
            base_config = MeshConfig.parse_obj(self.config_override)
        else:
            sqlmesh_path = Path(self.path)
            configs = load_configs(None, MeshConfig, [sqlmesh_path])
            if sqlmesh_path not in configs:
                raise ValueError(f"SQLMesh configuration not found at {sqlmesh_path}")
            base_config = configs[sqlmesh_path]

        config_dict = base_config.dict()

        if self.virtual_environment_mode is not None:
            config_dict["virtual_environment_mode"] = self.virtual_environment_mode
        if self.environment_catalog_mapping is not None:
            config_dict["environment_catalog_mapping"] = self.environment_catalog_mapping
        if self.default_target_environment is not None:
            config_dict["default_target_environment"] = self.default_target_environment

        return MeshConfig.parse_obj(config_dict)
