books = load 'webdam-books.txt' as (year: int, title: chararray, author: chararray) ;
flat= foreach books generate $1, $2;
dump flat;
