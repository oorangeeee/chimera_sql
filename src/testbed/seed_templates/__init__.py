"""种子 SQL 模板发现与注册。"""

from __future__ import annotations

import importlib
import pkgutil
from typing import List

from .base import SeedTemplate
from src.utils.logger import get_logger

logger = get_logger(__name__)


def discover_templates() -> List[SeedTemplate]:
    """自动发现并实例化所有 SeedTemplate 子类。

    扫描 seed_templates/dialect/ 和 seed_templates/standard/ 两个子包，
    导入所有模块，收集 SeedTemplate 的具体子类并实例化。
    """
    templates: List[SeedTemplate] = []
    seen_classes = set()

    for subpackage in ("dialect", "standard"):
        try:
            package = importlib.import_module(f"src.testbed.seed_templates.{subpackage}")
        except ImportError:
            logger.warning("模板子包不存在: %s", subpackage)
            continue

        package_path = getattr(package, "__path__", None)
        if package_path is None:
            continue

        for _importer, module_name, _ispkg in pkgutil.iter_modules(package_path):
            full_module = f"src.testbed.seed_templates.{subpackage}.{module_name}"
            try:
                mod = importlib.import_module(full_module)
            except Exception as e:
                logger.warning("导入模板模块失败 %s: %s", full_module, e)
                continue

            for attr_name in dir(mod):
                attr = getattr(mod, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, SeedTemplate)
                    and attr is not SeedTemplate
                    and attr not in seen_classes
                ):
                    seen_classes.add(attr)
                    try:
                        instance = attr()
                        templates.append(instance)
                    except Exception as e:
                        logger.warning("实例化模板 %s 失败: %s", attr.__name__, e)

    templates.sort(key=lambda t: t.category)
    logger.info("发现 %d 个种子模板", len(templates))
    return templates
