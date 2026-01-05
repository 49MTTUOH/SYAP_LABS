import re
from typing import Tuple


class OctaveToPythonTranslator:

    def __init__(self):
        self.patterns = {
            'matrix_def': r'\[([\d\s\.;,]+)\]',
            'range_def': r'(\d+)\s*:\s*(\d+)',
            'indexing': r'\b(\w+)\(([^)]+)\)',
        }

    def translate_matrix_definition(self, code: str) -> str:
        """Транслирует определение матриц Octave в NumPy массивы."""

        def matrix_replacer(match):
            matrix_str = match.group(1)
            rows = matrix_str.split(';')
            python_rows = []
            for row in rows:
                # Простое разделение чисел по пробелам
                elements = row.strip().split()
                python_rows.append('[' + ', '.join(elements) + ']')
            return f'np.array([{", ".join(python_rows)}])'

        return re.sub(self.patterns['matrix_def'], matrix_replacer, code)

    def translate_range_syntax(self, code: str) -> str:
        """Транслирует синтаксис диапазонов Octave."""

        def range_replacer(match):
            start = match.group(1)
            end = match.group(2)
            return f'np.arange({start}, {int(end) + 1})'

        return re.sub(self.patterns['range_def'], range_replacer, code)

    def translate_function_definitions(self, code: str) -> str:
        """Транслирует объявления функций Octave в Python функции."""
        lines = code.split('\n')
        result_lines = []

        for line in lines:
            stripped = line.strip()

            # Проверяем, является ли строка определением функции
            if stripped.startswith('function'):
                # Используем регулярное выражение для разбора
                match = re.match(r'function\s+(?:\[(.*?)\]\s*=\s*)?(\w+)\s*\((.*?)\)', stripped)
                if match:
                    outputs = match.group(1)  # [x, y]
                    func_name = match.group(2)  # solve_system
                    inputs = match.group(3)  # A, b

                    # Преобразуем в Python синтаксис
                    result_lines.append(f"def {func_name}({inputs}):")

                    # Если есть возвращаемые значения, добавляем комментарий
                    if outputs:
                        result_lines.append(f"    # Returns: {outputs}")
                else:
                    result_lines.append(line)
            else:
                result_lines.append(line)

        return '\n'.join(result_lines)

    def translate_multiple_returns(self, code: str) -> str:
        """Транслирует вызовы функций с несколькими возвращаемыми значениями."""
        lines = code.split('\n')
        result_lines = []

        for line in lines:
            # Ищем паттерн [var1, var2] = func(...)
            match = re.match(r'^\s*\[(.*?)\]\s*=\s*(\w+)\((.*?)\)', line.strip())
            if match:
                vars_str = match.group(1)  # solution, determinant
                func_name = match.group(2)  # solve_system
                args = match.group(3)  # M, [1; 2; 3]

                # В Python: solution, determinant = solve_system(M, ...)
                result_lines.append(f"{vars_str} = {func_name}({args})")
            else:
                result_lines.append(line)

        return '\n'.join(result_lines)

    def translate(self, octave_code: str) -> Tuple[str, bool]:
        """Основной метод трансляции."""
        python_code = octave_code

        # Цепочка преобразований
        transformations = [
            self.translate_function_definitions,
            self.translate_matrix_definition,
            self.translate_range_syntax,
            self.translate_multiple_returns,
        ]

        for transform in transformations:
            python_code = transform(python_code)

        # Удаляем "end" в конце функций
        python_code = python_code.replace('end', '')

        # Убираем лишние пустые строки
        lines = python_code.split('\n')
        cleaned_lines = []
        for i, line in enumerate(lines):
            if line.strip() or (i < len(lines) - 1 and lines[i + 1].strip()):
                cleaned_lines.append(line)
        python_code = '\n'.join(cleaned_lines)

        # Добавляем импорт NumPy
        if 'import numpy' not in python_code:
            python_code = f"import numpy as np\n\n{python_code}"

        return python_code


def main():
    """Демонстрация работы транслятора."""
    octave_example = """
function [x, y] = solve_system(A, b)
    M = [1 2 3; 4 5 6]
    r = 1:10
    element = A(2, 3)
    x = A * b
    y = det(A)
end

M = [1 2 3; 4 5 6; 7 8 9]
r = 1:10
element = M(2, 3)
[solution, determinant] = solve_system(M, [1; 2; 3])
"""

    translator = OctaveToPythonTranslator()
    python_code = translator.translate(octave_example)

    print("Оригинальный код Octave:")
    print(octave_example)
    print("\nПереведенный код Python:")
    print(python_code)



if __name__ == "__main__":
    main()