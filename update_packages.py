#!/usr/bin/env python3
"""
Script para actualizar las declaraciones de paquetes después de la migración.
Actualiza SOLO la línea 'package' de cada archivo, manteniendo todo lo demás intacto.
"""

import os
import re
from pathlib import Path

# Mapeo de directorios a paquetes
PACKAGE_MAPPINGS = {
    'domain': 'org.lareferencia.core.domain',
    'repository/jpa': 'org.lareferencia.core.repository.jpa',
    'repository/parquet': 'org.lareferencia.core.repository.parquet',
    'service/harvesting': 'org.lareferencia.core.service.harvesting',
    'service/validation': 'org.lareferencia.core.service.validation',
    'service/indexing': 'org.lareferencia.core.service.indexing',
    'service/management': 'org.lareferencia.core.service.management',
    'worker/harvesting': 'org.lareferencia.core.worker.harvesting',
    'worker/validation': 'org.lareferencia.core.worker.validation',
    'worker/validation/validator': 'org.lareferencia.core.worker.validation.validator',
    'worker/validation/transformer': 'org.lareferencia.core.worker.validation.transformer',
    'worker/indexing': 'org.lareferencia.core.worker.indexing',
    'worker/management': 'org.lareferencia.core.worker.management',
    'task': 'org.lareferencia.core.task',
}

def update_package_declaration(file_path, new_package):
    """Actualiza la declaración de paquete en un archivo Java."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Buscar y reemplazar la línea de package
    pattern = r'^package\s+org\.lareferencia\.(backend|core)\.[^;]+;'
    replacement = f'package {new_package};'
    
    new_content = re.sub(pattern, replacement, content, count=1, flags=re.MULTILINE)
    
    if new_content != content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return True
    return False

def process_directory(base_path, relative_path, new_package):
    """Procesa todos los archivos Java en un directorio."""
    full_path = os.path.join(base_path, relative_path)
    
    if not os.path.exists(full_path):
        return 0
    
    count = 0
    for file in os.listdir(full_path):
        if file.endswith('.java'):
            file_path = os.path.join(full_path, file)
            if update_package_declaration(file_path, new_package):
                count += 1
                print(f"  ✅ {relative_path}/{file}")
    
    return count

def main():
    # Base path
    base_path = '/Users/lmatas/source/lareferencia-platform/lareferencia-core-lib/src/main/java/org/lareferencia/core'
    
    total_updated = 0
    
    print("🔄 Actualizando declaraciones de paquetes...\n")
    
    for relative_path, new_package in PACKAGE_MAPPINGS.items():
        print(f"📦 {relative_path} → {new_package}")
        count = process_directory(base_path, relative_path, new_package)
        total_updated += count
    
    print(f"\n✨ Total de archivos actualizados: {total_updated}")

if __name__ == '__main__':
    main()
