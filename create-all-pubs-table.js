const fs   = require('fs');
const Database = require('better-sqlite3');
const db_root='db-tests/';

//creates or locates database file
const db = new Database(db_root+'all-pubs.db');

let publications = Array.from(JSON.parse(fs.readFileSync('pub-list.json', 'utf8')).publications);

let selections={
    "n1":[],
    "n2":[],
    "n3":[],
    "n4":[],
    "n5":[]
}


for(let pub of publications){
    

    for(let n=1;n<6;n++){
        let columns=Object.keys(db.prepare(`SELECT * FROM ${pub.key}_n${n}`).get());
        
        let months=[];
        for(let y=1975;y<2000;y++){
            for(let m=1;m<13;m++){
                let str=`m${y}_${m<10?'0':''}${m}`;
                let match=columns.indexOf(str);
                if(match>=0) months.push(str);
                else months.push(`NULL AS ${str}`)

            }
        }


        let select=`
        SELECT gram, ${months.join(', ')}
        FROM ${pub.key}_n${n}
        `;
        selections["n"+n].push(select);

    }
}



// console.log(selections.n1[0],selections.n1[1]);

let aggregates=[];
for(let y=1975;y<2000;y++){
    for(let m=1;m<13;m++){
        let str=`m${y}_${m<10?'0':''}${m}`;
        aggregates.push(`SUM(${str}) AS ${str}`);
    }
}


let statement_builder=(unions,n)=>{
    return `CREATE TABLE all_publications_n${n} AS SELECT gram, ${aggregates.join(', ')}
    FROM (
     ${unions.join('UNION ALL')}
     ) GROUP BY gram;`
}



console.log('beginning sum table creation')
for(let n=1;n<6;n++){
    console.log(`creating all_publications_n${n}...`);
    let statement=statement_builder(selections["n"+n],n);
    db.prepare(statement).run();
}
console.log('finished');


for(let n=1;n<6;n++){
    console.log(`creating index for all_publications_n${n}...`)
    db.prepare(`CREATE INDEX index_all_publications_n${n} ON all_publications_n${n} (gram);`).run();
}

console.log('vacuuming...')
db.prepare('VACUUM;').run();