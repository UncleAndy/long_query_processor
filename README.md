# long_query_processor - сервис для асинхронной обработки долгих запросов

## Общее описание

Сервис состоит из двух компонентов:

* `server.psgi` - интерфейс для взаимоджействия с сервером посредством API (см. далее);
* `query_processor.pl` - приложение, непосредственно, занимающееся обработкой запросов;

## Конфигурация

Описание параметров подключения к БД и параметров работы процессора запросов находятся в файле `config.yaml`. Он должен располагаться в том-же каталоге, в котором находятся приложения. В коде приведен пример конфигурации.

## PSGI сервер  

Пример запуска сервера: `starman --port 8080 --workers 1 ./server.psgi`

## Процессор запросов

Пример запуска: `./query_processor.pl`

## API

### Постановка запроса в очередь на исполнение

`POST http://127.0.0.1:8080/request`

Данные (JSON):

```json
{
  "period": "<период>",
  "owner_inn": "<inn владельца>",
  "owner_name": "<имя владельца с возможностью использовать '%'>",
  "type": <тип>,
  "contractor_inn": "<inn исполнителя>",
  "contractor_name": "<имя исполнителя с возможностью использовать '%'>",
  "date": "<дата>",
  "number": "<номер>"
}
```
Можно использовать любую комбинацию параметров. Если передать пустой JSON `{}` - в результат войдет весь список.

Ответ: 

`<номер запроса в очереди>`.

### Проверка состояния запроса и получение данных при его готовности

`GET http://127.0.0.1:8080/result?query_id=<номер запроса>`

Если запрос уже исполнен, в ответ будет выдаваться файл с именем `invoices_query_<номер запроса>.csv` со всеми данными в формате CSV.

Возможные коды ошибок HTTP в ответе:

* `452` - неверный формат параметра query_id;
* `404` - не существует запроса с таким query_id;
* `406` - запрос завершился с ошибкой. Текст ошибки - в теле ответа;
* `422` - обработка запроса не завершена;

Для всех этих кодов в теле ответа выдается текст с пояснением.
