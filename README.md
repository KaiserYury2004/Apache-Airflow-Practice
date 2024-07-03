# Apache-Airflow-Practice
В данном задании необходимо локально развернуть и настроить рабочее окружение со всеми необходимыми инструментами:
- Apache Airflow
- Pandas
- Python
- MongoDB

Далее стоит задача создать **DAG** (Directed Acyclic Graph) для обработки данных и загрузке их в Mongo DB,где будут реализованы необходимые пайплайны.

![image](https://github.com/KaiserYury2004/Apache-Airflow-Practice/assets/129221692/76143569-d54b-48a9-ba2a-53320d846906)

## Настройка среды
Дабы не вдаваться в полемику,пройдемся по этому кратко.
### Apache Airflow
Установка Apache Airflow процесс крайне ответственный,поэтому доверимся [индусу]([https://pages.github.com/](https://www.youtube.com/watch?v=v8gbHZbttGs&ab_channel=DataRollup)) и установим тул на **WSL**(Windows Subsystem for Linux).Перед использованием рекомендуется почитать инструкцию к эксплуатации.
### Python & Pandas
Тут думаю все очевидно и с этим справиться любой ^^
### MongoDB 
Данная NoSQL база данных может быть использована либо в облачном хранилище MongoDB Atlas,либо же на локальной машине через MongoCompass.Я использовал второй вариант.Переходим на официальный сайт и скачиваем MongoDB Shell на вашу версию Windows.
#### Подключение к MongoDB через Airflow
Т.к. изначально во вкладке Airflow Admin>>Connections MongoDB нету,используя Google находим инструкцию для airflow.cfg и настраиваем для нашей NoSQL.Далее добавляем в Connections подключение(если используете Atlas не забудьте про HTTPS подключение.

Заодно я вспомнил,что еще необходимо указать подключение к файловой системе (Connection Type - **File(path)**)

## Работа с кодом

Перед запуск рекомендуется проверка на то,что все используемые библиотеки установлены.
Также в переменной path_to_data следует указать путь до вашего csv-файла(путь указывается в соответствии с правилами Linux,т.к просто указать путь вряд-ли выйдет)
Однако не стоит забывать про имена баз данных и коллекций.Там придется указывать свои данные.

## DAG's
![image](https://github.com/KaiserYury2004/Apache-Airflow-Practice/assets/129221692/cfd0a7a1-a07e-4a7c-90e3-a57730a8e53d)

**Mongo_DAG_1** выполняет поиск и обработку данных,потом через Dataset запускает **Mongo_DAG_2**

Если все сделано верно и по инструкции,то должно получиться 

![image](https://github.com/KaiserYury2004/Apache-Airflow-Practice/assets/129221692/49bf2922-af50-486b-8ac1-5b98b978cc44)

![image](https://github.com/KaiserYury2004/Apache-Airflow-Practice/assets/129221692/2a841174-eb73-480e-8882-1d0627506936)



## Запросы в MongoDB 
После загрузки обработанных данных,предстоит написать пайплайны с запросами:
- Топ-5 часто встречаемых комментариев

Для этого надо создать 3 Stages:
1) Stage 1 - $group
{
  _id: "$content",
  count: {
    $sum: 1
  }
}
2) Stage 2 - $sort
   {
  count: -1
}
3) Stage 3 - $limit 5
- Все записи, где длина поля “content” составляет менее 5 символов;
1) Stage 1 - $match{$expr: {$lt: [{$strLenCP: "$content"},5]}}
- Средний рейтинг по каждому дню (результат должен быть в виде timestamp type)
1) Stage 1 - $group{
  _id: {
    year: {
      $year: {
        $toDate: "$at"
      }
    },
    month: {
      $month: {
        $toDate: "$at"
      }
    },
    day: {
      $dayOfMonth: {
        $toDate: "$at"
      }
    }
  },
  averageScore: {
    $avg: "$score"
  }
}
2) Stage 2 - $project{
  _id: 0,
  date: {
    $dateFromParts: {
      year: "$_id.year",
      month: "$_id.month",
      day: "$_id.day"
    }
  },
  averageScore: 1
}
3) Stage 3 - {
  date: 1
}

Однако есть более простая альтернатива,в MongoDB имеется консоль,через которую можно сделать все гораздно проще(файл с командами будет в этом же репозитории)
