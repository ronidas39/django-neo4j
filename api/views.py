from rest_framework.response import Response
from rest_framework.decorators import api_view
from neo4j import GraphDatabase
import sys
from typing import Dict


@api_view(["GET"])
def get(request):
    driver=GraphDatabase.driver(uri="bolt://localhost:7687",auth=("neo4j","Rambo@1234"))
    session=driver.session()
    q1="""
    Match(n) return count(n) as count
    """
    results=session.run(q1)
    li=[r["count"] for r in results]
    result={"count":li}
    return Response(result)

@api_view(["POST"])
def create_journal(request):
    driver=GraphDatabase.driver(uri="bolt://localhost:7687",auth=("neo4j","Rambo@1234"))
    session=driver.session()
    data=request.data
    keys=list(data.keys())
    print(keys)
    simple_keys=[]
    for key in keys:
    #print(type(data[key]))
        if isinstance(data[key],Dict) or isinstance(data[key],str):
            simple_keys.append(key)

    for simple_key in simple_keys: 
        print(simple_key)       
        if simple_key == "JournalPaper":
            rJournalReference=data[simple_key]
            referenceDOI=rJournalReference['doi']
            authorScopusID=rJournalReference['authorScopusID']
        elif simple_key=="Affiliation":
            rAffiliation=data[simple_key]
        elif simple_key=="Year":
            rYear=data[simple_key]
        elif simple_key=="Conceptual":
            rConceptual=data[simple_key]
        elif simple_key=="JournalPublication":
            rJournalPublication=data[simple_key]
        elif simple_key=="Publisher":
            rPublisher=data[simple_key]
            vPublisherName=rPublisher['name']
        elif simple_key=="Empirical":
            rEmpirical=data[simple_key]
        elif simple_key=="Keyword":
            rKeyword=data[simple_key]
            if rKeyword=="":
                rKeyword={}
        elif simple_key=="Funding":
            rFunding=data[simple_key]
        elif simple_key=="Data":
            rData=data[simple_key]
        elif simple_key=="Method":
            rMethod=data[simple_key]
        else:
            print(simple_key)
            print("invalid data")
            sys.exit()


    qall="""
merge (a:JournalReference {doi:$rJournalReference.doi}) 
on create 
set a=$rJournalReference
merge(b:Year {name:$rYear.name}) 
on create 
set b=$rYear
create(d:JournalPublication) set d=$rJournalPublication
create(e:Publisher) set e=$rPublisher
create(h:Funding) set e=$rFunding
create(i:Data) set i=$rData
create(j:Method) set j=$rMethod
create(a)-[:USED]->(i)
create(a)-[:IN]->(b)
create(a)-[:APPEARED_IN]->(d)
create(d)-[:PUBLISHED_BY]->(e)
create(a)-[:USED]->(j)
create(d)-[:USED]->(j)
create(d)-[:USED]->(i)
create(a)<-[:FUNDED]-(h)
"""
    dict_all={"rJournalReference":rJournalReference,"rYear":rYear,"rJournalPublication":rJournalPublication,"rPublisher":rPublisher,"rFunding":rFunding,"rData":rData,"rMethod":rMethod}

#dict_all={"rJournalReference":rJournalReference,"rYear":rYear,"rConceptual":rConceptual,"rJournalPublication":rJournalPublication,"rPublisher":rPublisher,"rEmpirical":rEmpirical,"rKeyword":rKeyword,"rFunding":rFunding,"rData":rData,"rMethod":rMethod}
    session.run(qall,dict_all)
# (JournalReference if isEmpirical =true)-[:HAS_TYPE]->(Empirical) 
# (JournalReference if  isConceptual =true )-[:HAS_TYPE]->(Conceptual)
# keyword match with doi to JournalReference [:HAS]
# match JournalReference authorScopusID with author [AUTHORED_BY]
# match Affiliation authorScopusID with author [WITH]

    complex_keys=[]
    for key in keys:
        if isinstance(data[key],list) and len(data[key])!=0:
            complex_keys.append(key)
    print(complex_keys)

    for complex_key in complex_keys:
        print(complex_key)
        if complex_key=="Author":
            print("here")
            authors=data[complex_key]
            print(authors)
            for author in authors:
                rAuthor=author
                qauthor="""
                create(a:Author) set a=$rAuthor
                """
                try:
                    dict={"rAuthor":rAuthor}
                    session.run(qauthor,dict)
                except Exception as e:
                    print(str(e))
        elif complex_key=="BibliographicReference":
            bibliographicReferences=data[complex_key]
            for bibliographicReference in bibliographicReferences:
                rBibliographicReference=bibliographicReference
                qbib="""
                create(a:BibliographicReference) set a=$rBibliographicReference
                """
                try:
                    dict={"rBibliographicReference":rBibliographicReference}
                    session.run(qbib,dict)
                except Exception as e:
                    print(str(e))
        elif complex_key=="Hypothesis":
            hypothesiss=data[complex_key]
            for hypothesis in hypothesiss:
                rHypothesis=hypothesis
                qhyp="""
                create(a:Hypothesis) set a=$rHypothesis
                """
                try:
                    dict={"rHypothesis":rHypothesis}
                    session.run(qhyp,dict)
                except Exception as e:
                    print(str(e))
        elif complex_key=="Proposition":
            propositions=data[complex_key]
            for proposition in propositions:
                rProposition=proposition
                qprop="""
                create(a:Proposition) set a=$rProposition
                """
                try:
                    dict={"rProposition":rProposition}
                    session.run(qprop,dict)
                except Exception as e:
                    print(str(e))
        elif complex_key=="Affiliation":
            Affiliations=data[complex_key]
            for Affiliation in Affiliations:
                rAffiliation=Affiliation
                qbib="""
                 create(a:Affiliation) set a=$rAffiliation
                """
                try:
                    dict={"rAffiliation":rAffiliation}
                    session.run(qbib,dict)
                except Exception as e:
                    print(str(e))
        elif complex_key=="Keyword":
            Keywords=data[complex_key]
            for Keyword in Keywords:
                rKeyword=Keyword
                qbib="""
                 create(a:Keyword) set a=$rKeyword
                """
                try:
                    dict={"rKeyword":rKeyword}
                    session.run(qbib,dict)
                except Exception as e:
                    print(str(e))
        elif complex_key=="Construct":
            Constructs=data[complex_key]
            for Construct in Constructs:
                rConstruct=Construct
                qbib="""
                 create(a:Construct) set a=$rConstruct
                """
                try:
                    dict={"rConstruct":rConstruct}
                    session.run(qbib,dict)
                except Exception as e:
                    print(str(e))
        else:
            print(complex_key)
            print("invalid data")
            sys.exit()

    dict={"referenceDOI":referenceDOI,"authorScopusID":authorScopusID,"vPublisherName":vPublisherName}    

    q="""MERGE (iv:`Construct Role`:`Independent Variable`)
MERGE (dv:`Construct Role`:`Dependent Variable`)
MERGE (mv1:`Construct Role`:`Moderator Variable`)
MERGE (mv2:`Construct Role`:`Mediator Variable`)"""

    session.run (q,dict)

    q="""match (k:Keyword),(j:JournalReference)
where k.doi=j.doi and j.doi=$referenceDOI
merge (j)-[:HAS]->(k)"""

    session.run (q,dict)

# q="""match (k:Funding),(j:JournalReference)
# where k.fundingID=j.fundingID and j.doi=$referenceDOI
# merge (j)<-[:FUNDED]-(k)"""

# session.run (q,dict)

    q="""match (k:Keyword ),(j:JournalReference)
where k.doi=j.doi and j.doi=$referenceDOI
merge (j)-[:HAS]->(k)"""

    session.run (q,dict)

    q="""match (a:Author), (j:JournalReference)
where a.scopusID in j.authorScopusID and a.scopusID in $authorScopusID
merge (j)-[:AUTHORED_BY]->(a)"""

    session.run (q,dict)

    q="""match (a:BibliographicReference), (j:JournalReference)
where a.citingDOI = j.doi and j.doi=$referenceDOI
merge (j)-[:CITED]->(a)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:AUTHORED_BY]->(a:Author), (af:Affiliation)
WHERE a.scopusID = af.authorScopusID  and j.doi=$referenceDOI
merge (af)-[:PRODUCED]->(j)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference), (h:Hypothesis)
WHERE j.doi=h.doi and j.doi=$referenceDOI 
merge (j)-[:STUDIED]->(h)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:AUTHORED_BY]->(a:Author), (p:Proposition)
WHERE a.scopusID in p.authorScopusID and j.doi=$referenceDOI and a.scopusID in $authorScopusID  
merge (j)-[:STUDIED]->(p)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference) -[:AUTHORED_BY]->(a:Author), (c:Construct)
WHERE a.scopusID in c.authorScopusID and j.doi=$referenceDOI and a.scopusID in $authorScopusID   
merge (j)-[:STUDIED]->(c)"""

    session.run (q,dict)

    q="""MATCH (a:Author), (p:Proposition)
WHERE a.scopusID = p.authorScopusID and a.scopusID in $authorScopusID   
merge (a)-[:STUDIED]->(p)"""

    session.run (q,dict)

    q="""MATCH (a:Author), (c:Construct)
WHERE a.scopusID = c.authorScopusID and a.scopusID in $authorScopusID    
merge (a)-[:STUDIED]->(c)"""

    session.run (q,dict)

    q="""MATCH (p:Publisher), (j:JournalPublication)
WHERE p.name = j.publisherName and p.name=$vPublisherName
MERGE (p)<-[:PUBLISHED_BY]-(j)"""

    session.run (q,dict)

    q="""MATCH (a:Author)<-[:AUTHORED_BY]-(j:JournalReference), (jp:JournalPublication)<-[:APPEARED_IN]-(j:JournalReference)
where j.doi=$referenceDOI
MERGE (a)-[:CONTRIBUTED_TO]->(jp)"""

    session.run (q,dict)

    q="""MATCH (a:Author)-[:CONTRIBUTED_TO]->(j:JournalPublication)-[:PUBLISHED_BY]->(p:Publisher)
where a.scopusID in $authorScopusID   
MERGE (a)-[:CONTRIBUTED_TO]->(p)"""

    session.run (q,dict)

    q="""MATCH (jp:JournalPublication)<-[:APPEARED_IN]-(j:JournalReference)-[:HAS]->(k:Keyword)
where j.doi=$referenceDOI
MERGE (jp)-[:HAS]->(k)"""

    session.run (q,dict)

    q="""MATCH (h:Hypothesis), (a:Author)
WHERE h.authorScopusID = a.scopusID and a.scopusID in $authorScopusID  
MERGE (h)<-[:STUDIED]-(a)"""

    session.run (q,dict)

    q="""MATCH (a:Author)-[:STUDIED]->(h:Hypothesis), (a)-[:CONTRIBUTED_TO]->(jp:JournalPublication)
where a.scopusID in $authorScopusID  
MERGE (jp)-[:STUDIED]->(h)"""

    session.run (q,dict)

    q="""MATCH (c:Construct), (iv:`Construct Role`:`Independent Variable`)
WHERE c.ConstructRole = 'IndependentVariable' and c.doi=$referenceDOI
MERGE (c)-[:AS]->(iv)"""

    session.run (q,dict)

    q="""MATCH (c:Construct), (dv:`Construct Role`:`Dependent Variable`)
WHERE c.ConstructRole = 'DependentVariable' and c.doi=$referenceDOI
MERGE (c)-[:AS]->(dv)"""

    session.run (q,dict)

    q="""MATCH (c:Construct), (mv:`Construct Role`:`Mediator Variable`)
WHERE c.ConstructRole = 'MediatorVariable' and c.doi=$referenceDOI
MERGE (c)-[:AS]->(mv)"""

    session.run (q,dict)

    q="""MATCH (c:Construct), (mv:`Construct Role`:`Moderator Variable`)
WHERE c.ConstructRole = 'ModeratorVariable' and c.doi=$referenceDOI
MERGE (c)-[:AS]->(mv)"""

    session.run (q,dict)

    q="""MATCH (jp:JournalPublication)<-[:APPEARED_IN]-(j:JournalReference)-[:STUDIED]->(c:Construct)
where j.doi=$referenceDOI
MERGE (jp)-[:STUDIED]->(c)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(c:Construct)-[:AS]->(iv:`Construct Role`:`Independent Variable`)
where j.doi=$referenceDOI
MERGE (j)-[:HAS]->(iv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(c:Construct)-[:AS]->(dv:`Construct Role`:`Dependent Variable`)
where j.doi=$referenceDOI
MERGE (j)-[:HAS]->(dv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(c:Construct)-[:AS]->(mv:`Construct Role`:`Mediator Variable`)
where j.doi=$referenceDOI
MERGE (j)-[:HAS]->(mv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(c:Construct)-[:AS]->(mv:`Construct Role`:`Moderator Variable`)
where j.doi=$referenceDOI
MERGE (j)-[:HAS]->(mv)"""

    session.run (q,dict)


    q="""MATCH (j:JournalReference)-[APPEARED_IN]->(jp:JournalPublication)-[:STUDIED]->(c:Construct)-[:AS]->(iv:`Construct Role`:`Independent Variable`)
where j.doi=$referenceDOI
MERGE (jp)-[:HAS]->(iv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[APPEARED_IN]->(jp:JournalPublication)-[:STUDIED]->(c:Construct)-[:AS]->(dv:`Construct Role`:`Dependent Variable`)
where j.doi=$referenceDOI
MERGE (jp)-[:HAS]->(dv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[APPEARED_IN]->(jp:JournalPublication)-[:STUDIED]->(c:Construct)-[:AS]->(mv:`Construct Role`:`Mediator Variable`)
where j.doi=$referenceDOI
MERGE (jp)-[:HAS]->(mv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[APPEARED_IN]->(jp:JournalPublication)-[:STUDIED]->(c:Construct)-[:AS]->(mv:`Construct Role`:`Moderator Variable`)
where j.doi=$referenceDOI
MERGE (jp)-[:HAS]->(mv)"""

    session.run (q,dict)

    q="""MATCH(f:Funding)-[:FUNDED]->(j:JournalReference)-[:AUTHORED_BY]->(a:Author)
where j.doi=$referenceDOI
MERGE (a)-[:FUNDED_BY]->(f)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(p:Proposition),(c:Construct)-[:AS]->(iv:`Construct Role`:`Independent Variable`)
where p.propositionID=c.propositionID and j.doi=$referenceDOI
MERGE (p)-[:HAS]->(iv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(p:Proposition),(c:Construct)-[:AS]->(dv:`Construct Role`:`Dependent Variable`)
where p.propositionID=c.propositionID and j.doi=$referenceDOI 
MERGE (p)-[:HAS]->(dv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(p:Proposition),(c:Construct)-[:AS]->(mv:`Construct Role`:`Mediator Variable`)
where p.propositionID=c.propositionID and j.doi=$referenceDOI  
MERGE (p)-[:HAS]->(mv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(p:Proposition),(c:Construct)-[:AS]->(mv2:`Construct Role`:`Moderator Variable`)
where p.propositionID=c.propositionID and j.doi=$referenceDOI  
MERGE (p)-[:HAS]->(mv2)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(h:Hypothesis),(c:Construct)-[:AS]->(iv:`Construct Role`:`Independent Variable`)
where h.hypothesisID=c.hypothesisID and j.doi=$referenceDOI
MERGE (h)-[:HAS]->(iv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(h:Hypothesis),(c:Construct)-[:AS]->(dv:`Construct Role`:`Dependent Variable`)
where h.hypothesisID=c.hypothesisID and j.doi=$referenceDOI
MERGE (p)-[:HAS]->(dv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(h:Hypothesis),(c:Construct)-[:AS]->(mv:`Construct Role`:`Mediator Variable`)
where h.hypothesisID=c.hypothesisID and j.doi=$referenceDOI
MERGE (p)-[:HAS]->(mv)"""

    session.run (q,dict)

    q="""MATCH (j:JournalReference)-[:STUDIED]->(h:Hypothesis),(c:Construct)-[:AS]->(mv2:`Construct Role`:`Moderator Variable`)
where h.hypothesisID=c.hypothesisID and j.doi=$referenceDOI
MERGE (p)-[:HAS]->(mv2)"""

    session.run (q,dict)

    q="""MATCH (d:Data)<-[:USED]-(j:JournalReference)-[:APPEARED_IN]->(jp:JournalPublication)
where j.doi=$referenceDOI
MERGE (d)<-[:USED]-(jp)"""

    session.run (q,dict)

    q="""MATCH (d:Data)<-[:USED]-(j:JournalReference)-[:AUTHORED_BY]->(a:Author)
where j.doi=$referenceDOI
MERGE (d)<-[:USED]-(a)"""

    session.run (q,dict)

    q="""MATCH (m:Method)<-[:USED]-(j:JournalReference)-[:APPEARED_IN]->(jp:JournalPublication)
where j.doi=$referenceDOI
MERGE (m)<-[:USED]-(jp)"""

    session.run (q,dict)

    q="""MATCH (m:Method)<-[:USED]-(j:JournalReference)-[:AUTHORED_BY]->(a:Author)
where j.doi=$referenceDOI
MERGE (m)<-[:USED]-(a)"""

    session.run (q,dict)

    q="""MATCH (k:Keyword)<-[:HAS]-(j:JournalReference)-[:AUTHORED_BY]->(a:Author)
where j.doi=$referenceDOI
MERGE (a)-[:HAS]->(k)"""

    session.run (q,dict)

    q="""MATCH (k:Keyword)<-[:HAS]-(j:JournalReference)-[APPEARED_IN]->(jp:JournalPublication)-[:PUBLISHED_BY]->(p:Publisher)
where j.doi=$referenceDOI
MERGE (p)-[:HAS]->(k)"""

    session.run (q,dict)

    q="""MATCH (p:Proposition)<-[:STUDIED]-(j:JournalReference)-[:AUTHORED_BY]->(a:Author)
where j.doi=$referenceDOI
MERGE (a)-[:STUDIED]->(p)"""

    session.run (q,dict)
    return Response({"status":"ok"})

