package nextflow.trace

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j;
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import org.openprovenance.prov.model.*
import org.openprovenance.prov.interop.InteropFramework;

import java.nio.file.Path
import java.nio.file.Paths


/**
 * Created by edgar on 10/05/18.
 */

@Slf4j
@CompileStatic
public class ProvObserver implements TraceObserver {

    //** PROV info **
    public static final String PROVBOOK_NS = "prov";
    public static final String PROVBOOK_PREFIX = "NF";

    private final ProvFactory pFactory= InteropFramework.newXMLProvFactory();
    private final Namespace ns;
    public ProvObserver() {
        ns=new Namespace();
        ns.addKnownNamespaces();
        ns.register(PROVBOOK_PREFIX, PROVBOOK_NS);
    }
    public QualifiedName qn(String n) {
        return ns.qualifiedName(PROVBOOK_PREFIX, n, pFactory);
    }

    Map<QualifiedName,Entity> inputEntityMap = new HashMap<QualifiedName,Entity>();
    Map<QualifiedName,Entity> outputEntityMap = new HashMap<QualifiedName,Entity>();
    Map<QualifiedName,Activity> activityMap = new HashMap<QualifiedName,Activity>();
    Map<QualifiedName,Used> usedMap = new HashMap<QualifiedName,Used>();



    //**  --  **

    @Override
    public void onFlowStart(Session session) {

    }

    @Override
    public void onFlowComplete() {              //when the full pipeline is DONE
        Document document = pFactory.newDocument();
        def fileName= "helloWorld.json"
        /**
         * Print the INPUT MAP
         */
        println "--onComplete INPUT Entity LIST: ${inputEntityMap.size()}"
        if (inputEntityMap.isEmpty()){
            println "INPUT is empty"
        }else {
            for (Map.Entry<QualifiedName, Entity> entity : inputEntityMap.entrySet()){
                document.getStatementOrBundle().add(entity.value)
                //print   "-->>value: ${entity.getKey()} \n" //+
                        //"-->Value: ${entity.value} \n"+
                        //"-->Label: ${entity.getLabel()} \n" +
                        //"-->Type: ${entity.getType()} \n"+
                        //"-->Kind: ${entity.getKind()} \n"+
                        //"-->Other: ${entity.getOther()} \n"+
                        //"-->Location: ${entity.getLocation()} \n"
            }
        }
        /**
         * Print the OUTPUT MAP
         */
        println "\n--onComplete OUTPUT Entity LIST: ${outputEntityMap.size()}"
        if (outputEntityMap.isEmpty()){
            println "OUTPUT is empty"
        }else {
            for (Map.Entry<QualifiedName, Entity> entity : outputEntityMap.entrySet()){
                document.getStatementOrBundle().add(entity.value)
                //print   "-->>value: ${entity.getKey()} \n" //+
                        //"-->Value: ${entity.value} \n"+
                        //"-->Label: ${entity.getLabel()} \n" +
                        //"-->Type: ${entity.getType()} \n"+
                        //"-->Kind: ${entity.getKind()} \n"+
                        //"-->Other: ${entity.getOther()} \n"+
                        //"-->Location: ${entity.getLocation()} \n"
            }
        }
        //ACTIVITY
        for (Map.Entry<QualifiedName,Activity> activity  : activityMap.entrySet()){
            document.getStatementOrBundle().add(activity.value)
        }
        //USED
        for (Map.Entry<QualifiedName,Used> used  : usedMap.entrySet()){
            document.getStatementOrBundle().add(used.value)
        }
        /**
         * Generate the PROV document
         */
        document.setNamespace(ns);
        //doConversion function
        InteropFramework intF=new InteropFramework();
        intF.writeDocument(fileName, document);
        intF.writeDocument(System.out, InteropFramework.ProvFormat.PROVN, document);
    }

    @Override
    public void onProcessCreate(TaskProcessor process) {

    }

    @Override
    public void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        //log.info "PROV - info - submit process > $handler"
        //log.debug "PROV - submit process > $handler"

    }

    @Override
    public void onProcessStart(TaskHandler handler, TraceRecord trace) {
        //get input files
        //log.info "PROV -info-  START process > $handler"
        //log.debug "PROV - submit process > $handler"

    }

    @Override
    public void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        /**
         * Code to generate the ACTITY object.
         * It's the object who represents the process itself
         */
        Activity activity_object = pFactory.newActivity(qn(trace.getTaskId().toString()));
        //TODO is it possible to add start/end time to the activity
        //activity_object.setStartTime(trace.getFmtStr("start")...)
        activityMap.put(activity_object.getId(), activity_object)
        /**
         * Get the I/O objects from the trace.
         * Convert the string into a List<String> to iterate it
         */
        def inputFiles = trace.getFmtStr("input")
        def outputFiles = trace.getFmtStr("output")
        List<String> inputList = Arrays.asList(inputFiles.split(";"));
        List<String> outputList = Arrays.asList(outputFiles.split(";"));
        int iteratAux=0;    //to generate IDs (used,...)
        //println "##  name:${trace.getFmtStr("name")} \n## processList size: ${inputList.size()}" //\n** ${inputList.toString()}"
        //println "\$\$ inputFiles : ${inputFiles}"
        /**
         * Iterate the INPUT list to get the values we need
         */
        for (elem in inputList) {
            Path pathAux = Paths.get(elem);
            String pathString = pathAux.toString().trim() //remove space from the begining of the path.. need to avoid uncomprensive behaviour
            def entity_name = pathAux.getFileName()
            //println "** onProcessComplete INPUT: ${entity_name} \n* PATH:${pathString}"
            //XXX check if the ELEM is already on the inputList (global) or not --> done with a MAP
            Entity input_entity = pFactory.newEntity(qn(pathString.toString()));                                      //setId()
            input_entity.setValue(pFactory.newValue(entity_name.toString(), pFactory.getName().PROV_VALUE))    //setValue()
            /*
            pFactory.addLabel(input_entity, "labelFOO", "ENG")
            URI uriAux = new URI('URI_Foo');

            //here we add on the "type" list
            pFactory.addType(input_entity, uriAux)
            String checkStr = "checksumFOO"

            Object checkAux = checkStr
            pFactory.addType(input_entity, checkAux, qn("checksum"))

            String sizeStr = "sizeFoo"
            Object sizekAux = sizeStr
            pFactory.addType(input_entity, sizekAux, qn("fileSize"))

            String typeStr = "fileFoo"
            Object typeAux = typeStr
            pFactory.addType(input_entity, typeAux, qn("fileType"))
            //Location locationAux = path as Location
            //input_entity.getLocation().add(locationAux)

            //Other otherAux
            //otherAux.setValue("hola")
            //input_entity.getOther().add(otherAux)

            //println "~~ add to LIST getId: ${input_entity.getId()}"// \n  PATH: ${path}"
*/

            /*
            Create the relation btwn the ACTIVITY and the ENTITY
             */
            Used usedAction=pFactory.newUsed(activity_object.getId(),input_entity.getId());
            usedAction.setId(qn("${pathString}_${iteratAux}"))
            println "USED ID ${usedAction.getId()}"
            usedMap.put(usedAction.getId(),usedAction)
            /*
            Save the input element as a ENTITY inside the GLOBAL list of the Input entities
             */
            inputEntityMap.put(input_entity.getId(), input_entity)
            iteratAux++
        }
        /**
         * Iterate the OUTPUT list to get the values we need
         */
        for (elem in outputList) {
            Path pathAux = Paths.get(elem);
            String pathString = pathAux.toString().trim() //remove space from the begining of the path.. need to avoid uncomprensive behaviour
            def entity_name = pathAux.getFileName()
            //println "** onProcessComplete INPUT: ${entity_name} \n* PATH:${pathString}"
            //XXX check if the ELEM is already on the inputList (global) or not --> done with a MAP
            Entity output_entity = pFactory.newEntity(qn(pathString.toString()));                                      //setId()
            output_entity.setValue(pFactory.newValue(entity_name.toString(), pFactory.getName().PROV_VALUE))    //setValue()

            /*pFactory.addLabel(input_entity, "labelFOO", "ENG")
            URI uriAux = new URI('URI_Foo');
            pFactory.addType(input_entity, uriAux)
            String checkStr = "checksumFOO"
            Object checkAux = checkStr
            pFactory.addType(input_entity, checkAux, qn("checksum"))
            String sizeStr = "sizeFoo"
            Object sizekAux = sizeStr
            pFactory.addType(input_entity, sizekAux, qn("fileSize"))
            String typeStr = "fileFoo"
            Object typeAux = typeStr
            pFactory.addType(input_entity, typeStr, qn("objectType"))
            Location locationAux = path as Location
            input_entity.getLocation().add(locationAux)*/

            //Other otherAux
            //otherAux.setValue("hola")
            //input_entity.getOther().add(otherAux)

            //println "~~ add to LIST getId: ${input_entity.getId()}"// \n  PATH: ${path}"

            /*
            Save the input element as a ENTITY inside the GLOBAL list of the Input entities
             */
            outputEntityMap.put(output_entity.getId(), output_entity)
        }

    }

    @Override
    public void onProcessCached(TaskHandler handler, TraceRecord trace) {

    }

    @Override
    public boolean enableMetrics() {
        return false;
    }
}
