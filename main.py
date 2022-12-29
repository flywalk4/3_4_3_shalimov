from multiprocessing import Pool, cpu_count
import csv
import os
import re
import matplotlib.pyplot as plt
import numpy as np
from os import path
from prettytable import PrettyTable
import pdfkit
import doctest
import concurrent.futures
from functools import partial
import chuncker
experienceToRus = {
    "noExperience": "Нет опыта",
    "between1And3": "От 1 года до 3 лет",
    "between3And6": "От 3 до 6 лет",
    "moreThan6": "Более 6 лет",
    "":""
}

experienceToPoints = {
    "noExperience": 0,
    "between1And3": 1,
    "between3And6": 2,
    "moreThan6": 3,
    "":""
}

grossToRus = {
    "true": "Без вычета налогов",
    "false": "С вычетом налогов",
    "True": "Без вычета налогов",
    "False": "С вычетом налогов",
    "":""
}

currencyToRus = {
    "AZN": "Манаты",
    "BYR": "Белорусские рубли",
    "EUR": "Евро",
    "GEL": "Грузинский лари",
    "KGS": "Киргизский сом",
    "KZT": "Тенге",
    "RUR": "Рубли",
    "UAH": "Гривны",
    "USD": "Доллары",
    "UZS": "Узбекский сум",
    "":""
}

fieldToRus = {
    "name": "Название",
    "description": "Описание",
    "key_skills": "Навыки",
    "experience_id": "Опыт работы",
    "premium": "Премиум-вакансия",
    "employer_name": "Компания",
    "salary": "Оклад",
    "salary_gross": "Оклад указан до вычета налогов",
    "salary_currency": "Идентификатор валюты оклада",
    "area_name": "Название региона",
    "published_at": "Дата публикации вакансии",
    "":""
}

def files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield path + "/" + file

def get_key(d, value):
    """Получает первый ключ по значению

    Args:
        d (dict): Словарь для поиска ключа
        value(object): Значение по которому искать
    >>> x = {1: 2, "3": "x", 4: "2"}
    >>> get_key(x, 2)
    1
    >>> x = {1: 2, "3": "x", 4: "2"}
    >>> get_key(x, "x")
    '3'
    >>> x = {1: 2, "3": "x", 4: "2"}
    >>> get_key(x, "2")
    4
    """
    for k, v in d.items():
        if v == value:
            return k

class Vacancy:
    """Класс для представления вакансии.

    Attributes:
        name (str): Название вакансии
        salary (Salary): Зарплата
        area_name (str): Город работы
        published_at (str): Дата публикации вакансии
    """
    def __init__(self, name : str, salary : float, area_name : str, published_at : str):
        """Инициализирует объект Vacancy, выполняет конвертацию дляполей.

            Args:
                name (str): Название вакансии
                salary (Salary): Зарплата
                area_name (str): Город работы
                published_at (str): Дата публикации вакансии
        """
        self.name = name
        self.salary = salary
        self.area_name = area_name
        self.published_at = published_at

    def date_to_string(self):
        """Переводит аттрибут published_at класса Vacancy в формат dd.mm.yyyy

            Returns:
                str: Дата в формате dd.mm.yyyy
            
        >>> Vacancy("x", "<br><b>x</b>yz</br>", 'z', "noExperience", "true", "x", Salary("100", "2000", "true", "RUR"), "x", "2007-12-03T17:40:09+0300").date_to_string()
        '03.12.2007'
        >>> Vacancy("x", "<br><b>x</b>yz</br>", 'z', "noExperience", "true", "x", Salary("100", "2000", "true", "RUR"), "x", "2012-10-03T17:12:09+0300").date_to_string()
        '03.10.2012'
        """
        splitted_date = self.published_at.split("T")[0].split("-")
        date_string = splitted_date[2] + "." + splitted_date[1] + "." + splitted_date[0]
        return date_string

    def date_get_year(self):
        """Получить год публикации вакансии

            Returns:
                int: Год публикации вакансии
            
        >>> Vacancy("x", "<br><b>x</b>yz</br>", 'z', "noExperience", "true", "x", Salary("100", "2000", "true", "RUR"), "x", "2007-12-03T17:40:09+0300").date_get_year()
        2007
        >>> Vacancy("x", "<br><b>x</b>yz</br>", 'z', "noExperience", "true", "x", Salary("100", "2000", "true", "RUR"), "x", "2012-10-03T17:12:09+0300").date_get_year()
        2012
        """
        return int(self.date_to_string().split(".")[-1])

class HtmlGenerator:
    """Класс для генерации HTML страницы
    """
    def generate_table(self, titles, content):
        """Возвращает HTML код таблицы

            Args:
                titles (list): Заголовки столбцов
                content (list): Строки таблицы

            Returns:
                str: HTML код таблицы
        """
        table = "<table>"
        table += self.generate_titles(titles)
        for row in content:
            table += self.generate_row(row)
        table += "</table>"
        return table

    def generate_titles(self, titles):
        """Возвращает HTML код для заголовков таблицы

            Args:
                titles (list): Заголовки столбцов

            Returns:
                str: HTML код заголовков
        """
        string = "<tr>"
        for title in titles:
            string += "<th>" + title + "</th>"
        string += "</tr>"
        return string

    def generate_row(self, row):
        """Возвращает HTML код для строки таблицы

            Args:
                row (list): Строка таблицы

            Returns:
                str: HTML код для строки
        """
        string = "<tr>"
        for row_item in row:
            string += "<td>" + str(row_item) + "</td>"
        string += "</tr>"
        return string

    def generate_html(self, dicts, image_path, prof_name, city_name):
        """Возвращает HTML код страницы с графиками и 3-мя таблицами 

            Args:
                dicts (list): Словари со строками и заголовками для таблиц
                image_path (str): Путь до графика
                prof_name (str): Имя выбранной профессии
            
            Returns:
                str: HTML код страницы
        """
        html = """<!DOCTYPE html>
                    <html lang="en">
                    <head>
                        <meta charset="UTF-8">
                        <title>Report</title>
                    </head>
                    <style>
                    body{
                        font-family: Verdana;
                    }
                    table{
                        text-align: center;
                        border-collapse: collapse;
                    }
                    th, td{
                        border: 1px solid;
                        padding: 5px;
                    }
                    </style>
                    <body>
                    <h1 style="text-align: center; font-size: 60px;">Аналитика по зарплатам и городам для профессии """ + prof_name + """</h1>
                    <img src=\"""" + image_path + """\">"""
        # 1
        titles = ["Год", "Средняя зарплата - " + prof_name, "Количество вакансий - " + prof_name]
        html += f"<h1 style='text-align:center;'>Статистика по годам для города {city_name}</h1>"
        html += "<table style='width: 100%;'>" + self.generate_titles(titles)
        dict = dicts[0]
        for i in range(len(dict[0])):
            year = dict[0][i]
            avgSalaryProf = list(dict[3].values())[i]
            vacAmountProf = list(dict[4].values())[i]
            row = [year, avgSalaryProf, vacAmountProf]
            html += self.generate_row(row)
        html += """</table> <br>"""

        # 2
        titles = ["Город", "Уровень зарплат"]
        html += "<h1 style='text-align:center;'>Статистика по городам</h1>"
        html += "<table style='float: left; width: 45%;'>" + self.generate_titles(titles)
        dict = dicts[1][0]
        values = list(dict.values())
        keys = list(dict.keys())
        for i in range(len(values)):
            city = keys[i]
            avgSalary = values[i]
            row = [city, avgSalary]
            html += self.generate_row(row)
        html += "</table>"

        # 3
        titles = ["Город", "Доля вакансий"]
        html += "<table style='float: right; width: 45%;'>" + self.generate_titles(titles)
        dict = dicts[1][1]
        values = list(dict.values())
        keys = list(dict.keys())
        for i in range(len(values)):
            city = keys[i]
            percent = str(values[i] * 100).replace(".", ",") + "%"
            row = [city, percent]
            html += self.generate_row(row)
        html += "</table></body></html>"
        return html
            
class Report:
    """Класс для создания графиков

        Attributes:
            filename (str): Имя файла
            html (str): HTML код страницы
    """
    def __init__(self, name, dicts, prof_name, city_name):
        """Инициализирует объект Report, генерирует граф и создает HTML код страницы
            Args:
                name (str): Имя файла
                dicts (list): Данные для графиков и таблиц
                prof_name (str): Имя выбранной профессии
        """
        generator = HtmlGenerator()
        parent_dir = path.dirname(path.abspath(__file__))
        self.filename = name
        self.generate_graph(dicts, prof_name, city_name)
        self.html = generator.generate_html(dicts, parent_dir + '/temp.png', prof_name, city_name)

    def generate_graph(self, dicts, prof_name, city_name):
        """Создает и сохраняет в виде файла графики

            Args:
                dicts (list): Данные для графиков
                prof_name (str): Имя выбранной профессии
        """
        dictsSalary = dicts[0]
        dictsCities = dicts[1]
        years = dictsSalary[0]
        plt.grid(axis='y')
        plt.style.use('ggplot')
        plt.rcParams.update({'font.size': 8})

        x = np.arange(len(years))
        width = 0.35
        ax = plt.subplot(2, 2, 1)
        ax.bar(x - width / 2, dictsSalary[1].values(), width, label='средняя з/п')
        ax.bar(x + width / 2, dictsSalary[3].values(), width, label='з/п ' + prof_name)
        ax.legend()
        ax.set_xticks(x, years, rotation=90)
        plt.title("Уровень зарплат по годам для \n"+city_name)

        ax = plt.subplot(2, 2, 2)
        ax.bar(x - width / 2, dictsSalary[2].values(), width, label='Количество вакансий')
        ax.bar(x + width / 2, dictsSalary[4].values(), width, label='Количество вакансий\n' + prof_name)
        ax.legend()
        ax.set_xticks(x, years, rotation=90)
        plt.title("Количество вакансий по годам для \n"+city_name)

        plt.subplot(2, 2, 3)
        plt.barh(list(reversed(list(dictsCities[0].keys()))), list(reversed(dictsCities[0].values())), alpha=0.8, )
        plt.title("Уровень зарплат по городам")

        plt.subplot(2, 2, 4)
        plt.pie(list(dictsCities[1].values()) + [1 - sum(list(dictsCities[1].values()))],
                labels=list(dictsCities[1].keys()) + ["Другие"])
        plt.title("Доля вакансий по городам")
        plt.subplots_adjust(wspace=0.5, hspace=0.5)

        plt.savefig("temp.png", dpi=200, bbox_inches='tight')

class DataSet:
    """Класс для хранения названия файла и всех вакансий

        Attributes:
            file_name (str): Имя файла
            vacancies_objects (list): Вакансии
    """
    def __init__(self, ﬁle_name: str, vacancies_objects: list):
        """Инициализирует объект DataSet

        Args:
            ﬁle_name (str): Имя файла
            vacancies_objects (list): Вакансии
        """
        self.file_name = file_name
        self.vacancies_objects = vacancies_objects

class CSVReader:
    def csv_ﬁler(self, vacancy_in, fields):
        """Создает вакансию, находя необходимые аттрибуты для нее

            Args:
                vacancy_in (list): Вакансия в виде list

            Returns:
                Vacancy: Вакансия
        """
        name = vacancy_in[fields.index("name")] if "name" in fields else ""
        area_name = vacancy_in[fields.index("area_name")] if "area_name" in fields else ""
        salary = vacancy_in[fields.index("salary")]
        published_at = vacancy_in[fields.index("published_at")] if "published_at" in fields else ""
        vacancy = Vacancy(name, salary, area_name, published_at)
        return vacancy   



    def get_vacancies(self, ﬁle_name):
        """Считывает все вакансии с файла

            Args:
                filename (str): Название файла
            
            Returns:
                [int, list] : Массив из года вакансий и самих вакансий
        """
        vacancies = []
        fields = []
        with open(ﬁle_name, encoding="UTF-8-sig") as File:
            reader = csv.reader(File, delimiter=',')
            for row in reader:
                if (fields == []):
                    fields = row
                else:
                    vacancy = self.csv_ﬁler(row, fields)
                    vacancies.append(vacancy)
                    year = vacancy.date_get_year()
            File.close()
        return [year, vacancies]

class DataWorker:
    """Класс для статистической обработки вакансий
    """
    def get_data(self, prof_name, vacancies_objects, city_name):
        """Обрабатывает вакансии и возвращает статистические данные

            Args:
                vacancies_objects (list): Список вакансий
                prof_name (str): Имя выбранной профессии
            
            Returns:
                dict: Статистические данные
        """
        year = vacancies_objects[0]
        vacancies_objects = vacancies_objects[1]
        salary_out = []
        amount_out = 0
        salary_prof_out = []
        amount_prof_out = 0
        cities_salary = {}
        cities_amount = {}
        for vacancy in vacancies_objects:
            if vacancy.salary == "":
                continue
            if float(vacancy.salary) > 20000000:
                continue    
            avg_salary = float(vacancy.salary)
            if prof_name in vacancy.name and vacancy.area_name == city_name:
                # Динамика уровня зарплат по годам
                salary_out += [avg_salary]
                # Динамика количества вакансий по годам
                amount_out += 1
                
                # Динамика уровня зарплат по годам для выбранной профессии и региона
                salary_prof_out += [avg_salary]
                # Динамика количества вакансий по годам для выбранной профессии и региона
                amount_prof_out += 1

            # Уровень зарплат по городам (в порядке убывания)
            if vacancy.area_name not in cities_salary:
                cities_salary[vacancy.area_name] = [avg_salary]
            else:
                cities_salary[vacancy.area_name] += [avg_salary]
            # Доля вакансий по городам (в порядке убывания)
            if vacancy.area_name not in cities_amount:
                cities_amount[vacancy.area_name] = 1
            else:
                cities_amount[vacancy.area_name] += 1
        return [year, salary_out, amount_out, salary_prof_out, amount_prof_out, cities_salary, cities_amount]

def print_data(data, total_vacancies):
    """Обрабатывает вакансии и возвращает словари для создания таблиц, графиков и выводит данные этих словарей

            Args:
                data (list): Статистические данные
                total_vacancies (int): Общеее число вакансий
            
            Returns:
                [dict, dict]: Данные для создания таблиц и графиков
        """
    temp = {}
    salaryDict = []
    cityDict = []
    for x in data["salary"].keys():
        if len(data["salary"][x]) == 0:
            temp[x] = 0
            continue
        temp[x] = int(sum(data["salary"][x]) / len(data["salary"][x]))
    print("Динамика уровня зарплат по годам:", temp)
    salaryDict.append(list(list(data["salary"].keys())[i] for i in range(len(data["salary"].keys()))))
    salaryDict.append(temp)
    print("Динамика количества вакансий по годам:", data["amount"])
    salaryDict.append(data["amount"])
    temp = {list(data["salary"].keys())[i]: 0 for i in range(len(data["salary"].keys()))}
    for x in data["salary_prof"].keys():
        if len(data["salary"][x]) == 0:
            temp[x] = 0
            continue
        temp[x] = int(sum(data["salary_prof"][x]) / len(data["salary_prof"][x]))
    print("Динамика уровня зарплат по годам для выбранной профессии:", temp)
    salaryDict.append(temp)

    if len(data["amount_prof"]) != 0:
        print("Динамика количества вакансий по годам для выбранной профессии:", data["amount_prof"])
        salaryDict.append(data["amount_prof"])
    else:
        temp = {list(data["salary"].keys())[i]: 0 for i in range(len(data["salary"].keys()))}
        print("Динамика количества вакансий по годам для выбранной профессии:", temp)

        salaryDict.append(temp)

    temp = {}
    if "Россия" in data["salary_city"]:
        data["salary_city"].pop("Россия")
    for x in data["salary_city"].keys():
        percent = len(data["salary_city"][x]) / total_vacancies
        if (percent >= 0.01):
            temp[x] = int(sum(data["salary_city"][x]) / len(data["salary_city"][x]))
    temp = dict(sorted(temp.items(), key=lambda x: x[1], reverse=True)[:10])
    print("Уровень зарплат по городам (в порядке убывания):", temp) 
    cityDict.append(temp)
    temp = {}
    if "Россия" in data["amount_city"]:
        data["amount_city"].pop("Россия")
    for x in data["amount_city"].keys():
        percent = data["amount_city"][x] / total_vacancies
        if (percent >= 0.01):
            temp[x] = round(percent, 4)
    temp = dict(sorted(temp.items(), key=lambda x: x[1], reverse=True)[:10])
    print("Доля вакансий по городам (в порядке убывания):", temp)
    cityDict.append(temp)
    return [salaryDict, cityDict]

def main_futures(file_names, prof_name, city_name):
    """Обрабатывает и считывает вакансии в многопоточном режиме

        Args:
            file_names(list): Названия файлов
            prof_name (str): Имя выбранной профессии
    """
    def read_get_data(prof_name, city_name, file_name):
        dataWorker = DataWorker()
        csvReader = CSVReader()
        vacancies = csvReader.get_vacancies(file_name)
        return [dataWorker.get_data(prof_name, vacancies, city_name), len(vacancies[1])]

    years = []
    total_vacancies = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        queue = {executor.submit(partial(read_get_data, prof_name, city_name), file_name): file_name for file_name in file_names}
        for answer in concurrent.futures.as_completed(queue):
            result = answer.result()
            years.append(result[0])
            total_vacancies += result[1]
            years = sorted(years, key=lambda year: year[0])

    cities_salary = {}
    cities_amount = {}
   
    for year in years:
        city_salary = year[5]
        for city in city_salary:
            city = city
            if city not in cities_salary:
                cities_salary[city] = city_salary[city]
            else:
                cities_salary[city] += city_salary[city]

        city_amount = year[6]
        for city in city_amount:
            if city not in cities_amount:
                cities_amount[city] = city_amount[city]
            else:
                cities_amount[city] += city_amount[city]

    dict = {"salary": {x[0]:x[1] for x in years},
            "amount": {x[0]:x[2] for x in years},
            "salary_prof": {x[0]:x[3] for x in years},
            "amount_prof": {x[0]:x[4] for x in years},
            "salary_city": cities_salary,
            "amount_city": cities_amount}
    options = {'enable-local-file-access': None}
    config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
    report = Report("graph.jpg", print_data(dict, total_vacancies), prof_name, city_name)
    pdfkit.from_string(report.html, 'report.pdf', configuration=config, options=options)

if __name__ == "__main__":
    file_name = input("Введите название файла: ")
    prof_name = input("Введите название профессии: ")
    city_name = input("Введите название города: ")
    chuncker.сsv_chuncker(file_name)
    main_futures(list(files("csv")), prof_name, city_name)
