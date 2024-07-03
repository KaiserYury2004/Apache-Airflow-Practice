##Console query
##--1 Топ-5 часто встречаемых комментариев
db.your_collection_name.aggregate([
  {
    $group: {
      _id: "$content",
      count: { $sum: 1 }
    }
  },
  {
    $sort: { count: -1 }
  },
  {
    $limit: 5
  }
]).pretty();
##--2 Все записи, где длина поля “content” составляет менее 5 символов;
db.your_collection_name.aggregate([
  {
    $match: {
      $expr: {
        $lt: [
          { $strLenCP: "$content" },
          5
        ]
      }
    }
  }
]).pretty();
##--3 Средний рейтинг по каждому дню (результат должен быть в виде timestamp type).
db.your_collection_name.aggregate([
  {
    $group: {
      _id: {
        year: { $year: { $toDate: "$at" } },
        month: { $month: { $toDate: "$at" } },
        day: { $dayOfMonth: { $toDate: "$at" } }
      },
      averageScore: { $avg: "$score" }
    }
  },
  {
    $project: {
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
  },
  {
    $sort: { date: 1 }
  }
]).pretty();
