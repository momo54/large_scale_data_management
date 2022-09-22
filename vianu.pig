books  = load 'webdam-books.txt' as (year: int, title: chararray, author: chararray) ;
vianu = filter books by author == 'Vianu';
publishers = load 'webdam-publishers.txt' as (title: chararray, publisher: chararray) ;
joined = join vianu by title, publishers by title;
dump joined;
grouped = group joined by vianu::author;
dump grouped;

