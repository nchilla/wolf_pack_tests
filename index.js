const fs = require('fs');

cleanFile('sample-data/des-moines-register-clean.json',false,false);
cleanFile('sample-data/des-moines-register-abbrev.json',true,false);
cleanFile('sample-data/des-moines-register-trim.json',true,true);


async function testRead(){
    let rawdata = await fs.readFileSync('sample-data/des-moines-register.json');
    let parsed = await JSON.parse(rawdata);
    return parsed["{'Des Moines Register'}"];
}

async function cleanFile(output,abbrev,trim){
    let data=await testRead();
    let cleaned={};
    for(let month of data){
        let key=Object.keys(month)[0];
        let grams=month[key];
        let newGrams={};

        if(abbrev){
            for(let n=1;n<6;n++){
                let val=grams[n+' gram'];
                if(trim){
                    val=await trim1Counts(val);
                }

                newGrams[n]=val;
            }


            
        }else{
            newGrams=grams;
        }

        cleaned[key]=newGrams;
        
    }
    

    await fs.writeFile(output, JSON.stringify(cleaned), err => {
        if (err) {
          console.error(err)
          return
        }
    })
    console.log('complete');


}


async function trim1Counts(grams){
    let newGrams={};
    for(let gram of Object.keys(grams)){
        if(grams[gram]>1){
            console.log(gram,grams[gram]);
            newGrams[gram]=await grams[gram];
        }

    }
    return newGrams;
}