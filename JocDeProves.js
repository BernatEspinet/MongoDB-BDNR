use MongoDB-BDNR

//1. Escàners diferents que hi ha a la BD. Mostra el device.
db.CTScanner.distinct("Device")

//2. Número total de nòduls que s’han utilitzat per l’entrenament (train=1) de l’experiment 1 del mètode "Method2”.
db.Relation.aggregate({$match: {Train: 1, ExperimentRepetition:1, "MethodID":"Method2"}}, {$count: "NodulID"})

//3. Valor màxim, mínim i mitjà de BenignPrec agrupat per classificador (classifier).Mostra ID del mètode, MaxBenignPrec, MinBenignPrec, AvgBenignPrec. 

db.Experiment.aggregate([
    {$unwind: "$Method"},
    {$group: {_id: "$Method.Classifier",
    MaxBenignPrec: {$max: "$BenignPrec"},
                 MinBeningPrec: {$min: "$BenignPrec"},
                 AvgBeningPrec: {$avg: "$BenignPrec"}}}
    ])
//4. Numero total d’homes i dones. Mostra sexe i número total. 
db.Patient.aggregate({$group: {_id:"$Gender", count:{$sum:1}}})

//5. Pacients amb més de dos nòduls. Mostra ID del Pacient, sexe, edat, diagnòstic del Pacient 

db.Nodule.aggregate({$group:{_id:"$PatientID", count:{$sum:1}}}, {$match: {count:{$gt: 2}}}, 
                    {$lookup:{
                    from:"Patient",
                    localField: "_id",
                    foreignField: "_id",
                    pipeline:[{$project: {Gender:1, Age:1, DiagnosisPatient:1,_id:0}}],
                    as: "info_patient"}}, {$unwind: "$info_patient"},{$project:{_id:1, "info_patient.Gender":1, "info_patient.Age":1, "info_patient.DiagnosisPatient":1}})

//6. Mostrar els 4 mètodes amb més repeticions de l’experiment. Mostra el ID del Mètode i número de repeticions de l’experiment. 

db.Experiment.aggregate({$unwind: "$Method"},{$group:{_id: "$Method.MethodID", count:{$sum:1}}},{$sort:{count:-1}}, {$limit: 4})

//7. Per cada pacient els escàners (CTs) que s’ha fet. Mostra el ID del Pacient,device i la data del CT.
db.Nodule.aggregate({$lookup:{
                    from:"CTScanner",
                    localField: "CTScanner.CTID",
                    foreignField: "CTID",
                    pipeline:[{$project:{Device:1, dataCT:1, _id:0}}],
                    as: "info_ct"}}, {$unwind: "$info_ct"}, {$group: {_id:"$PatientID", device:{$addToSet:"$info_ct.Device"}, dataCT: {$addToSet: "$info_ct.dataCT"}}}, 
                    {$unwind:"$device"},
                    {$unwind:"$dataCT"})
//8. Mostrar els pacients que tenen tots els seus nóduls amb diagnosis = “Benign” i el seu recompte.
db.Nodule.aggregate({$match: {"DiagnosisPatient":"Benign", "DiagnosisNodul": "Benign"}}, {$group: {_id:"$PatientID", recompte:{$sum:1}}})


//9. Modificar la ResolutionTV aumentant-la un 20% dels escàners que es van realitzar amb DataCT = 18/11/2018 
db.CTScanner.update({dataCT: ISODate("2018-11-18")},{$mul: {ResolutionTV: 1.2}})
db.CTScanner.find()