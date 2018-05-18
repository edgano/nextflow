package nextflow.trace

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j;
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import  org.openprovenance.prov.model.*

import java.nio.file.Path


/**
 * Created by edgar on 10/05/18.
 */

@Slf4j
@CompileStatic
public class ProvObserver implements TraceObserver {

    //PROV info
    private final ProvFactory pFactory;
    private final Namespace ns;

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
        println "INPUT TRACE: ${trace.getFmtStr("input")}"
        /*def inputFile= trace.get('input')
        for (element in inputFile){
            def element_name = (element as Path).getFileName()
            Entity element_name_entity = pFactory.newEntity(qn(element_name));
            //check if it's a file
                element_name_entity.setValue(pFactory.newValue("file",pFactory.getName().PROV_TYPE));
                element_name_entity.setValue(pFactory.newValue("config", pFactory.getName().PROV_VALUE))
        }*/

    }

    @Override
    public void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        //get output files
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
