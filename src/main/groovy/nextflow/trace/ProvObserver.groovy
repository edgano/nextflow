package nextflow.trace

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j;
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import  org.openprovenance.prov.model.*

import javax.xml.bind.JAXBException
import java.nio.file.Path
import java.nio.file.Paths


/**
 * Created by edgar on 10/05/18.
 */

@Slf4j
@CompileStatic
public class ProvObserver implements TraceObserver {

    //PROV info
    private final ProvFactory pFactory
    private final Namespace ns
    public static final String PROVBOOK_PREFIX = "provbook";

    //singleton
    /*public ProvObserver(){
        //this.pFactory=pFactory
        ns=new Namespace();
        ns.addKnownNamespaces();
    }*/

    public QualifiedName qn(String n) {
        return ns.qualifiedName(PROVBOOK_PREFIX, n, pFactory);
    }


    @Override
    public void onFlowStart(Session session) {

    }

    @Override
    public void onFlowComplete() {

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
        //get output files
        //println "INPUT TRACE onComplete INPUT: ${trace.getFmtStr("input")}"
        //println "INPUT TRACE onComplete OUTPUT: ${trace.getFmtStr("output")}"

        def inputFiles= trace.getFmtStr("input")
        def outputFiles=trace.getFmtStr("output")
        List<String> inputList = Arrays.asList(inputFiles.split(";"));
        List<String> outputList = Arrays.asList(outputFiles.split(";"));
        List <Element> elementList = new ArrayList<Element>()


        for (element in inputList){

            Path path = Paths.get(element);
            def element_name = path.getFileName()
            //println "onComplete INPUT: ${element_name} \n ** PATH: ${path}"

            Entity input_element = pFactory.newEntity(qn(element_name.toString()))
            //asume it is a file
            //input_element.setValue(pFactory.newValue("file",pFactory.getName().PROV_TYPE));
            //input_element.setValue(pFactory.newValue("${element_name}", pFactory.getName().PROV_VALUE))
        }
        /*for (element in outputList){
            Path path = Paths.get(element);
            def element_name = path.getFileName()
            println "onComplete OUTPUT: ${element_name} \n ** PATH: ${path}" //use workdir as a PATH??
        }*/

        println "INPUT ELEMENT LIST: "
        for (element in pFactory){
            println " ${element}.getName() -=- ${element}.getProperties()"
        }

        /**
         * for each output element
         * ->
         * Entity multiqc_entity = pFactory.newEntity(qn("multiqc_file"));
         * multiqc_entity.setValue(pFactory.newValue("file",pFactory.getName().PROV_TYPE));
         * multiqc_entity.setValue(pFactory.newValue("config", pFactory.getName().PROV_VALUE))
         */
    }

    @Override
    public void onProcessCached(TaskHandler handler, TraceRecord trace) {

    }

    @Override
    public boolean enableMetrics() {
        return false;
    }
}
