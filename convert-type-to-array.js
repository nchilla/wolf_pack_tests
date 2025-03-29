const fs = require('fs');

let pub_list_json='../wolf-pack/static/data/publications.json';

let publications = Array.from(JSON.parse(fs.readFileSync(pub_list_json, 'utf8')).publications);

let publications_with_types_array=publications.map(({name,key,id,type})=>{
    return {
        name,
        key,
        id,
        type:'publication',
        tags:[type || 'print']
    }
})

fs.writeFileSync('pub-list-tags.json', JSON.stringify(publications_with_types_array));

// console.log(publications_with_types_array)
console.log('done');
