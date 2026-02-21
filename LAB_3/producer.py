from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.core.window import Window
from kivy.uix.button import Button
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.textinput import TextInput
from kivy.uix.scrollview import ScrollView
from kivy.uix.popup import Popup
import tkinter as tk
from tkinter import filedialog
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

Window.size = (800, 600)
Window.clearcolor = (144 / 255, 223 / 255, 255 / 255, 0.2 / 255)
Window.title = "Отправить в Kafka"

class Producer(App):
    def __init__(self):
        super().__init__()
        self.table = []
        self.cols = 3
        self.rows = 0
        self.selected_file = None

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka producer успешно подключен")
        except Exception as e:
            print(f"Ошибка подключения к Kafka: {e}")
            self.producer = None

    def build(self):
        main_layout = BoxLayout(orientation='vertical')
        control_panel = BoxLayout(size_hint_y=0.1, padding=10, spacing=10)
        add_row_btn = Button(text="Добавить строку", background_color=(255 / 255, 47 / 255, 108 / 255, 0.5))
        add_col_btn = Button(text="Добавить столбец", background_color=(255 / 255, 47 / 255, 108 / 255, 0.5))
        del_row_btn = Button(text="Удалить строку", background_color=(255 / 255, 47 / 255, 108 / 255, 0.5))
        del_col_btn = Button(text="Удалить столбец", background_color=(255 / 255, 47 / 255, 108 / 255, 0.5))

        add_row_btn.bind(on_press=self.add_row)
        add_col_btn.bind(on_press=self.add_col)
        del_row_btn.bind(on_press=self.del_row)
        del_col_btn.bind(on_press=self.del_col)

        control_panel.add_widget(add_row_btn)
        control_panel.add_widget(add_col_btn)
        control_panel.add_widget(del_row_btn)
        control_panel.add_widget(del_col_btn)

        name_layout = BoxLayout(size_hint_y=0.08, size_hint_x=0.3, padding=10, spacing=10)
        self.table_name = TextInput(hint_text="Название таблицы")
        name_layout.add_widget(self.table_name)

        table_container = BoxLayout(orientation="vertical", size_hint_y=0.7, padding=10)
        self.scroll = ScrollView(size_hint=(1, 1), do_scroll_x=True, do_scroll_y=True)

        self.table_layout = GridLayout(cols=1, spacing=1, size_hint=(None, None))
        self.table_layout.bind(minimum_height=self.table_layout.setter('height'))
        self.table_layout.bind(minimum_width=self.table_layout.setter('width'))

        self.scroll.add_widget(self.table_layout)
        table_container.add_widget(self.scroll)

        file_layout = BoxLayout(size_hint_y=0.12, padding=10)
        add_file_btn = Button(text="Загрузить файл", size_hint_y=0.8, size_hint_x=0.2, padding=10)
        add_file_btn.bind(on_press=self.select_file)
        self.file_name = Label(text="")

        send_btn = Button(text="Отправить", size_hint_y=0.8, size_hint_x=0.15)
        send_btn.bind(on_press=self.send_data)

        file_layout.add_widget(add_file_btn)
        file_layout.add_widget(self.file_name)
        file_layout.add_widget(send_btn)

        main_layout.add_widget(control_panel)
        main_layout.add_widget(name_layout)
        main_layout.add_widget(table_container)
        main_layout.add_widget(file_layout)

        return main_layout

    def select_file(self, instance):
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.askopenfilename(
            title="Выберите файл",
            filetypes=[("CSV files", "*.csv"), ("JSON files", "*.json")]
        )
        root.destroy()

        if file_path:
            self.selected_file = file_path
            file_name = file_path.split('/')[-1].split('\\')[-1]
            self.file_name.text = file_name
        else:
            self.selected_file = None

    def send_data(self, instance):
        if self.selected_file:
            if self.selected_file.endswith('.csv'):
                result = self.validate_csv(self.selected_file)
                print(result)
            elif self.selected_file.endswith('.json'):
                result = self.validate_json(self.selected_file)
                print(result)

            if result:
                # Отправка в Kafka
                if self.producer:
                    try:
                        future = self.producer.send('etl-topic', result)
                        future.get(timeout=10)  # Ждем подтверждения
                        print("Сообщение отправлено в Kafka")
                        self.show_message("Успех", "Файл прошел валидацию и отправлен в Kafka")
                    except KafkaError as e:
                        print(f"Ошибка отправки в Kafka: {e}")
                        self.show_message("Ошибка", f"Не удалось отправить в Kafka: {e}")
                        return
                else:
                    self.show_message("Ошибка", "Kafka producer не инициализирован")
                    return

                self.selected_file = None
                self.file_name.text = ""
            else:
                self.show_message("Ошибка", "Файл не прошел валидацию")
        else:
            result = self.validate_custom_table()
            print(result)
            if result:
                # Отправка в Kafka
                if self.producer:
                    try:
                        future = self.producer.send('etl-topic', result)
                        future.get(timeout=10)
                        print("Сообщение отправлено в Kafka")
                        self.show_message("Успех", "Таблица прошла валидацию и отправлена в Kafka")
                    except KafkaError as e:
                        print(f"Ошибка отправки в Kafka: {e}")
                        self.show_message("Ошибка", f"Не удалось отправить в Kafka: {e}")
                else:
                    self.show_message("Ошибка", "Kafka producer не инициализирован")
            else:
                self.show_message("Ошибка", "Таблица не прошла валидацию")

    def show_message(self, title, message):
        content = BoxLayout(orientation='vertical', padding=10)
        content.add_widget(Label(text=message))
        close_btn = Button(text='OK', size_hint_y=0.3)
        content.add_widget(close_btn)

        popup = Popup(title=title, content=content, size_hint=(0.6, 0.4))
        close_btn.bind(on_press=popup.dismiss)
        popup.open()

    def add_row(self, *args):
        row = BoxLayout(size_hint_y=None, height=40, spacing=1, size_hint_x=None)
        row.bind(minimum_width=row.setter('width'))
        self.rows += 1
        available_width = self.scroll.width if hasattr(self, 'scroll') and self.scroll.width > 0 else 800
        col_width = max(available_width / self.cols, 100)
        for col in range(self.cols):
            if self.rows == 1:
                cell = TextInput(multiline=False, text="", width=col_width, size_hint_x=None, background_normal="",
                                 background_color=(0.2, 0.6, 0.2, 1))
            else:
                cell = TextInput(multiline=False, text="", width=col_width, size_hint_x=None)
            row.add_widget(cell)
        self.table.append(row)
        self.table_layout.add_widget(row)

    def add_col(self, *args):
        self.cols += 1
        available_width = self.scroll.width if hasattr(self, 'scroll') and self.scroll.width > 0 else 800
        col_width = max(available_width / self.cols, 100)
        first_row = self.table[0]
        for row in self.table:
            old_table = [child.text for child in row.children[::-1]]
            row.clear_widgets()
            for i in range(self.cols):
                text = old_table[i] if i < len(old_table) else ""
                if row == first_row:
                    cell = TextInput(multiline=False, text=text, width=col_width, size_hint_x=None,
                                     background_normal="", background_color=(0.2, 0.6, 0.2, 1))
                else:
                    cell = TextInput(multiline=False, text=text, width=col_width, size_hint_x=None)
                row.add_widget(cell)

    def del_row(self, *args):
        if self.rows > 1:
            row = self.table[-1]
            self.table_layout.remove_widget(row)
            del (self.table[-1])
            self.rows -= 1

    def del_col(self, *args):
        if self.cols > 1:
            self.cols -= 1
            available_width = self.scroll.width if hasattr(self, 'scroll') and self.scroll.width > 0 else 800
            col_width = max(available_width / self.cols, 100)
            first_row = self.table[0]
            for row in self.table:
                old_table = [child.text for child in row.children[::-1]]
                row.clear_widgets()
                for i in range(self.cols):
                    if row == first_row:
                        cell = TextInput(multiline=False, text=old_table[i], width=col_width, size_hint_x=None,
                                         background_normal="", background_color=(0.2, 0.6, 0.2, 1))
                    else:
                        cell = TextInput(multiline=False, text=old_table[i], width=col_width, size_hint_x=None)
                    row.add_widget(cell)

    def create_json_message(self, cols_type, valid_lines):
        table_name = self.table_name.text.strip() if self.table_name.text.strip() else "untitled"

        cols_names = valid_lines[0] if isinstance(valid_lines[0], list) else []
        valid_lines = valid_lines[1:]

        message = {
            "table_name": table_name,
            "columns": [],
            "rows": []
        }

        for i, col_name in enumerate(cols_names):
            if i < len(cols_type):
                message["columns"].append({
                    "name": col_name if col_name else f"column_{i}",
                    "type": cols_type[i]
                })

        for row in valid_lines:
            row_data = []
            for i, value in enumerate(row):
                if i < len(cols_type):
                    if cols_type[i] == "int":
                        row_data.append(int(value))
                    elif cols_type[i] == "float":
                        row_data.append(float(value))
                    elif cols_type[i] == "bool":
                        if value.lower() in ['true']:
                            row_data.append(True)
                        else:
                            row_data.append(False)
                    else:
                        row_data.append(str(value))

            message["rows"].append(row_data)

        return message

    def validate_json(self, file_name):
        try:
            with open(file_name, 'r', encoding='utf-8') as file:
                data = json.load(file)

            if not isinstance(data, dict):
                print("JSON is not a dictionary")
                return None

            required_fields = ["table_name", "columns", "rows"]
            for field in required_fields:
                if field not in data:
                    print(f"Missing required field: {field}")
                    return None

            table_name = data["table_name"]
            columns = data["columns"]
            rows = data["rows"]

            if not isinstance(columns, list) or len(columns) == 0:
                print("Columns must be a non-empty list")
                return None

            validated_columns = []
            for col in columns:
                if not isinstance(col, dict):
                    print("Each column must be a dictionary")
                    return None

                if "name" not in col or "type" not in col:
                    print("Each column must have 'name' and 'type'")
                    return None

                valid_types = ["int", "float", "str", "bool"]
                if col["type"] not in valid_types:
                    print(f"Invalid type '{col['type']}' for column '{col['name']}'")
                    return None

                validated_columns.append({
                    "name": col["name"],
                    "type": col["type"]
                })

            if not isinstance(rows, list):
                print("Rows must be a list")
                return None

            validated_rows = []
            errors = []

            for i, row in enumerate(rows):
                if not isinstance(row, list):
                    errors.append(f"Row {i}: must be a list")
                    continue

                if len(row) != len(columns):
                    errors.append(f"Row {i}: not enough values")
                    continue

                validated_row = []
                is_row_valid = True

                for j in range(len(row)):
                    value, col = row[j], validated_columns[j]
                    try:
                        if col["type"] == "int":
                            validated_row.append(int(value))

                        elif col["type"] == "float":
                            validated_row.append(float(value))

                        elif col["type"] == "bool":
                            if isinstance(value, bool):
                                validated_row.append(value)
                            elif isinstance(value, str):
                                if value.lower() in ['true']:
                                    validated_row.append(True)
                                else:
                                    validated_row.append(False)
                            else:
                                validated_row.append(bool(value))

                        else:
                            if value is None:
                                validated_row.append("")
                            else:
                                validated_row.append(str(value))

                    except (ValueError, TypeError) as e:
                        errors.append(
                            f"Row {i}, col {j}: cannot convert '{value}' to {col['type']}")
                        is_row_valid = False
                        break

                if is_row_valid:
                    validated_rows.append(validated_row)


            if not validated_columns or not validated_rows:
                return None

            message = {
                "table_name": table_name,
                "columns": validated_columns,
                "rows": validated_rows,
            }

            if errors:
                error_msg = "\n".join(errors[:3])
                if len(errors) > 3:
                    error_msg += f"\n... and {len(errors) - 3} more errors"
                self.show_message("Валидация завершена с ошибками",
                                  f"Обработано {len(validated_rows)} из {len(rows)} строк\n\nОшибки:\n{error_msg}")

            return message

        except json.JSONDecodeError as e:
            error_msg = f"Неверный формат JSON: {str(e)}"
            self.show_message("Ошибка JSON", error_msg)
            return None
        except Exception as e:
            error_msg = f"Ошибка чтения файла: {str(e)}"
            self.show_message("Ошибка", error_msg)
            return None

    def validate_custom_table(self):
        if len(self.table) == 0:
            return None

        file_lines = []
        for row in self.table:
            row_data = [child.text for child in row.children[::-1]]
            file_lines.append(row_data)

        cols_type, valid_lines = self.validate_table(file_lines)
        if len(valid_lines) != 0:
            return self.create_json_message(cols_type, valid_lines)
        else:
            return None

    def validate_table(self, file_lines):
        if len(file_lines) < 2:
            return [], []

        cols_names = file_lines[0]
        cols_num = len(cols_names)
        cols_type = []

        for col in range(cols_num):
            type_choose = {"int": 0, "float": 0, "bool": 0, "str": 0}
            for row_idx in range(1, len(file_lines)):
                try:
                    num = float(file_lines[row_idx][col])
                    if num == int(num):
                        type_choose["int"] += 1
                    else:
                        type_choose["float"] += 1
                except (ValueError, IndexError):
                    if col < len(file_lines[row_idx]):
                        value_lower = file_lines[row_idx][col].lower()
                        if value_lower in ["false", "true"]:
                            type_choose["bool"] += 1
                        else:
                            type_choose["str"] += 1

            types_num = 0
            max_type = ""
            for type_name in type_choose.keys():
                if type_choose[type_name] > types_num:
                    types_num = type_choose[type_name]
                    max_type = type_name
            if max_type != "":
                cols_type.append(max_type)

        valid_lines = [cols_names]
        errors = []
        for row_idx in range(1, len(file_lines)):
            row = file_lines[row_idx]
            if len(row) != cols_num:
                errors.append(f"Row {row_idx}: must have {cols_num} values, but have {len(row)}")
                continue
            is_valid = True
            for col in range(cols_num):
                type_name = ""
                try:
                    num = float(row[col])
                    if num == int(num):
                        type_name = "int"
                    else:
                        type_name = "float"
                except ValueError:
                    value_lower = row[col].lower()
                    if value_lower in ["false", "true"]:
                        type_name = "bool"
                    else:
                        type_name = "str"

                if file_lines[row_idx][col] == "":
                    is_valid = False
                    errors.append(f"Row {row_idx}: empty value in column '{col}'")
                    break

                if type_name != cols_type[col]:
                    is_valid = False
                    errors.append(f"Row {row_idx}: value doesn't match '{type_name}' in column '{col}'")
                    break

            if is_valid:
                valid_lines.append(row)

        if errors:
            error_msg = "\n".join(errors[:3])
            if len(errors) > 3:
                error_msg += f"\n... and {len(errors) - 3} more errors"
            self.show_message("Валидация завершена с ошибками",
                              f"Обработано {len(valid_lines)} из {len(file_lines)} строк\n\nОшибки:\n{error_msg}")

        if len(valid_lines) > 1:
            return cols_type, valid_lines
        else:
            return [], []

    def validate_csv(self, file_name):
        file_lines = []
        try:
            file = open(file_name, encoding='utf-8')
            for line_file in file:
                file_lines.append([line.strip() for line in line_file.split(";")])
            file.close()
        except Exception as e:
            print(f"Error reading file: {e}")
            return None

        cols_type, valid_lines = self.validate_table(file_lines)

        if len(valid_lines) != 0:
            return self.create_json_message(cols_type, valid_lines)
        else:
            return None

    def on_stop(self):
        if self.producer:
            self.producer.close()
            print("Kafka producer закрыт")


if __name__ == "__main__":
    Producer().run()