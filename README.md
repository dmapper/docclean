# docclean

## clean old documents and corresponding ops-collection in mongodb

## Instalation

```bash
npm install -g docclean
```

## Usage

### as a cli tool

```bash
Usage: docclean -u [url] -d [num] -c [collections]

Options:
  -u, --url       mongodb url                                         [required]
  -d, --days      amount of days to preserve records                [default: 7]
  -c, --collections  collections to clean                             [required]

Examples:
  docclean -u mongodb://localhost:27017/idg -d 3 -c tasks
     
  # clean all 3-day old records from all tasks collections and corresponding ops
  # docs 
```          
               
### as a package

``` js
var cleaner = require('docclean');

cleaner('mongodb:/localhost:27017/mydb', 7, ['tasks'], function(err, results){
  // ...
});
```
                                  
