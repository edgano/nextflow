package nextflow.trace

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j;
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.util.Duration
import org.openprovenance.prov.model.*
import org.openprovenance.prov.interop.InteropFramework

import javax.xml.datatype.DatatypeFactory
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import java.text.SimpleDateFormat


/**
 * Created by edgar on 10/05/18.
 */

@Slf4j
@CompileStatic
public class ProvObserver implements TraceObserver {
    enum ProvenanceType{
        activityType, fileChecksum, fileSize, fileName
    }

    //** PROV info **
    public static final String PROVBOOK_NS = "prov";
    public static final String PROVBOOK_PREFIX = "PROV";

    private final String CHECKSUM_TYPE="SHA-256"
    private final String provFileName="helloWorld.json"
    private final String activity_prefix="activity_"
    private final String used_prefix="used_"
    private final String generatedBy_prefix="generatedBy_"

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

    Map<QualifiedName,Entity> inputEntityMap = new HashMap<QualifiedName,Entity>(); //TODO Can merge both entity maps (I guess)
    Map<QualifiedName,Entity> outputEntityMap = new HashMap<QualifiedName,Entity>();
    Map<QualifiedName,Activity> activityMap = new HashMap<QualifiedName,Activity>();
    Map<QualifiedName,Used> usedMap = new HashMap<QualifiedName,Used>();
    Map<QualifiedName,WasGeneratedBy> generatedMap = new HashMap<QualifiedName,WasGeneratedBy>();

    //RO structure names
    String roFolderName = "ro"
    String annotationFolderName = "annotation"
    String snapshotFolderName = "snapshot"
    String metadataFileName = "metadata.xml"
    String provenanceFileName = "provenance.json"
    String commandLineFileName = "commandLine.txt"
    String enviromentFileName = "enviroment.txt"
    String manifestFileName = "manifest.txt"
    String logFileName = "log.txt"

    @Override
    public void onFlowStart(Session session) {
        /**
         * Create the folder structure for the ResearchObject
         */
        //http://mrhaki.blogspot.com/2016/05/groovy-goodness-creating-files-and.html
        //Create the folder structure
        final FileTreeBuilder treeBuilder = new FileTreeBuilder(new File(roFolderName))
        treeBuilder.dir(annotationFolderName)
        treeBuilder.dir(snapshotFolderName)
        treeBuilder.file(metadataFileName)    // dockerSHA256 (X), software version, CommandLine, UUID, NF_version
        treeBuilder.file(manifestFileName)      //manifestInfo(X), author(X), date(X)
        treeBuilder.file(logFileName)           // append date+...
        //treeBuilder.file("${annotationFolderName}/${provenanceFileName}")
        //treeBuilder.file("${snapshotFolderName}/${commandLineFileName}")
        //copy main.nf to /snapshot --> get RunName?
        //treeBuilder.file("${snapshotFolderName}/${enviromentFileName}") //nextflow.config

        /**
         * Get the values from the config file
         * --> author
         * --> manifest
         * --> container
         */
        /*println "**** CONFIG ****\n"+
                "MANIFEST author: ${session.config.get("manifest").getAt("author")}\n"+
                "MANIFEST manifest: ${session.config.get("manifest").getAt("manifest")}\n"+
                "PROCESS containter: ${session.config.get("process").getAt("container")}\n"+
                "DOCKER:  ${session.config.get("docker")}\n"+
                "SINGULARITY:  ${session.config.get("singularity")}\n"+
                "****        ****"
         */
        /*for (Map.Entry<String, ArrayList<String>> entry : session.config.entrySet()) {
            println "KEY: ${entry.getKey()} -- VALUE: ${entry.getValue()}"
        }*/
        boolean dockerImage
        boolean singularityImage

        //for Metadata
        String containerName="** NOT container used **"
        String containerSha="** NOT container used **"
        String containerTechAux ="** NOT container used **"
        String dockerSHAPrefix ="sha256:"
        String singularitySHAPrefix=""
        String commandLine=""
        String UUID=""
        String nextflowVersion=""

        //for Manifest
        String authorAux ="** NOT provided **"
        String manifestAux ="** NOT provided **"
        def today = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date())
        //TODO log registry ?? --> append date

        if(session.config.containsKey("manifest")&& !session.config.get("manifest").getAt("author").toString().equals("null")){
            authorAux=session.config.get("manifest").getAt("author")
        }
        if(session.config.containsKey("manifest")&& !session.config.get("manifest").getAt("manifest").toString().equals("null")){
            manifestAux=session.config.get("manifest").getAt("manifest")
        }
        if (session.config.containsKey("singularity")&& session.config.get("singularity").getAt("enabled").toString().equals("true")){
            singularityImage=true
            containerTechAux = "Singularity"
        }else if(session.config.containsKey("docker")&& session.config.get("docker").getAt("enabled").toString().equals("true")){
            dockerImage=true
            containerTechAux = "Docker"
        }
        if(session.config.containsKey("process")&& !session.config.get("process").getAt("container").toString().equals("null")){
            containerName=session.config.get("process").getAt("container")
        }

        /**
         * Get container sha256 value
         * at SingularityCache.groovy -> runCommand
         */
        String cmd =""
        String duration='10min'

        if(dockerImage==true){  //https://stackoverflow.com/questions/32046334/where-can-i-find-the-sha256-code-of-a-docker-image
            //give a diff sha256 the .repoDigest and the Id value --> TO CHECK
            cmd ="docker inspect --format='{{index .Id}}' ${containerName}"
        }else if(singularityImage==true){
            //TODO does it work on macOX ??
            //ERROR --> DONT like it! it takes a while to digest the sha256
            // get the value when we pull the image
            //singularity pull --hash shub://vsoch/hello-world
            //Progress |===================================| 100.0%
            // Done. Container is at: /home/vanessa/ed9755a0871f04db3e14971bec56a33f.simg                      <-- file hash
            // use RGEGISTRY (singularity global client) : sregistry inspect $IMAGE -> version
            cmd = "sha256sum ./work/singularity/cbcrg-regressive-msa-v0.2.6.img | cut -d' ' -f1" //${containerName}
        }
        if(dockerImage || singularityImage){
            final max = Duration.of(duration).toMillis()
            //println "command to run: ${cmd}"
            final builder = new ProcessBuilder(['bash','-c',cmd])
            //builder.directory(storePath.toFile())
            //builder.environment().remove('SINGULARITY_PULLFOLDER')
            final proc = builder.start()
            final err = new StringBuilder()
            //*********START read proc stdout
            //https://stackoverflow.com/questions/8149828/read-the-output-from-java-exec
            builder.redirectErrorStream(true)
            BufferedReader ine;
            ine = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line;
            //print("sha256: > ")
            while ((line = ine.readLine()) != null) {
                containerSha=line
                // remove the prefix for the sha256 information
                if(dockerImage){
                    containerSha=containerSha.substring(dockerSHAPrefix.size())
                }else{
                    containerSha=containerSha.substring(singularitySHAPrefix.size())
                }
                //System.out.println(containerSha);
            }
            ine.close();
            //*******END read proc stdout
            proc.consumeProcessErrorStream(err)
            proc.waitForOrKill(max)
            def status = proc.exitValue()
            if( status != 0 ) {
                def msg = "Failed to get image info\n  command: $cmd\n  status : $status\n  message:\n"
                msg += err.toString().trim().indent('    ')
                throw new IllegalStateException(msg)
            }
        }

        /**
         * save data into manifest.xml
         */

        File metadataFile = new File("${treeBuilder.getBaseDir()}/${metadataFileName}")
        metadataFile.write "Container technology: ${containerTechAux}\n"
        metadataFile << "Container name: ${containerName}\n"
        metadataFile << "Container SHA256: ${containerSha}\n"
        //println metaFile.text

        File manifestFile = new File("${treeBuilder.getBaseDir()}/${manifestFileName}")
        manifestFile.write "Author: ${authorAux}\n"
        manifestFile << "Manifest: ${manifestAux}\n"
        manifestFile << "Date: ${today}\n"

        File logFile = new File("${treeBuilder.getBaseDir()}/${logFileName}")
        logFile.append("\n${today}")
    }

    @Override
    public void onFlowComplete() {              //when the full pipeline is DONE
        Document document = pFactory.newDocument();
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
        //WAS GENERATED BY
        for (Map.Entry<QualifiedName,WasGeneratedBy> generated  : generatedMap.entrySet()){
            document.getStatementOrBundle().add(generated.value)
        }
        /**
         * Generate the PROV document
         */
        /*document.setNamespace(ns);
        //doConversion function
        InteropFramework intF=new InteropFramework();
        intF.writeDocument(provFileName, document);
        intF.writeDocument(System.out, InteropFramework.ProvFormat.PROVN, document);*/
    }

    @Override
    public void onProcessCreate(TaskProcessor process) {

    }

    @Override
    public void onProcessSubmit(TaskHandler handler, TraceRecord trace) {

    }

    @Override
    public void onProcessStart(TaskHandler handler, TraceRecord trace) {

    }

    @Override
    public void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        /**
         * Code to generate the ACTITY object.
         * It's the object who represents the process itself
         */
        String activityId = "${activity_prefix}${trace.getTaskId()}"
        Activity activity_object = pFactory.newActivity(qn(activityId.toString()));

        String typeAux="Process"
        Object typeObj = typeAux
        pFactory.addType(activity_object, typeObj, qn(ProvenanceType.activityType.toString()))

        pFactory.addLabel(activity_object, trace.get("name").toString())

        /**
         * add start adn end time to the activity
         */
        //convert miliseconds to Gregorain
        final GregorianCalendar calendarGregStart = new GregorianCalendar();
        calendarGregStart.setTimeInMillis(trace.get("start") as long);
        def gregorianStart= DatatypeFactory.newInstance().newXMLGregorianCalendar(calendarGregStart);
        activity_object.setStartTime(gregorianStart)

        final GregorianCalendar calendarGregEnd = new GregorianCalendar();
        calendarGregEnd.setTimeInMillis(trace.get("complete") as long);
        def gregorianEnd= DatatypeFactory.newInstance().newXMLGregorianCalendar(calendarGregEnd);
        activity_object.setEndTime(gregorianEnd)

        activityMap.put(activity_object.getId(), activity_object)
        /**
         * Get the I/O objects from the trace.
         * Convert the string into a List<String> to iterate it
         */
        def inputFiles = trace.getFmtStr("input")
        def outputFiles = trace.getFmtStr("output")
        List<String> inputList = Arrays.asList(inputFiles.split(";"));
        List<String> outputList = Arrays.asList(outputFiles.split(";"));

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
            input_entity.setValue(pFactory.newValue(entity_name.toString(), qn(ProvenanceType.fileName.toString())))    //setValue()

            //pFactory.addLabel(input_entity, "labelFOO", "ENG")

            //here we add on the "type" list
            //URI uriAux = new URI('URI_Foo');
            //pFactory.addType(input_entity, uriAux)

            File fileAux = new File(pathString)
            //-- Digest sha256
            // https://gist.github.com/ikarius/299062/85b6540c99878f50f082aaee236ef15fc78e527c
            MessageDigest digest = MessageDigest.getInstance(CHECKSUM_TYPE);
            fileAux.withInputStream(){is->
                byte[] buffer = new byte[8192]
                int read = 0
                while( (read = is.read(buffer)) > 0) {
                    digest.update(buffer, 0, read);
                }
            }
            byte[] elementHash = digest.digest()
            //--
            Object checkAux = Base64.getEncoder().encodeToString(elementHash).toString()
            pFactory.addType(input_entity, checkAux, qn(ProvenanceType.fileChecksum.toString()))

            Object sizeAux = fileAux.length()
            pFactory.addType(input_entity, sizeAux, qn(ProvenanceType.fileSize.toString()))

            //Object typeEntity = elem.getClass().toString()
            //pFactory.addType(input_entity, typeEntity, qn("elementType"))

            //Location locationAux = path as Location
            //input_entity.getLocation().add(locationAux)

            //Other otherAux
            //otherAux.setValue("hola")
            //input_entity.getOther().add(otherAux)


            /*
             Create the relation btwn the ACTIVITY and the ENTITY
              */
            Used usedAction=pFactory.newUsed(activity_object.getId(),input_entity.getId());
            usedAction.setId(qn("${used_prefix}${trace.getTaskId()}_${pathString}"))
            usedMap.put(usedAction.getId(),usedAction)

            /*
            Save the input element as a ENTITY inside the GLOBAL list of the Input entities
             */
            inputEntityMap.put(input_entity.getId(), input_entity)
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
            output_entity.setValue(pFactory.newValue(entity_name.toString(), qn(ProvenanceType.fileName.toString())))   //setValue()

            File fileAux = new File(pathString)
            //-- Digest sha256
            // https://gist.github.com/ikarius/299062/85b6540c99878f50f082aaee236ef15fc78e527c
            MessageDigest digest = MessageDigest.getInstance(CHECKSUM_TYPE);
            fileAux.withInputStream(){is->
                byte[] buffer = new byte[8192]
                int read = 0
                while( (read = is.read(buffer)) > 0) {
                    digest.update(buffer, 0, read);
                }
            }
            byte[] elementHash = digest.digest()
            //--
            Object checkAux = Base64.getEncoder().encodeToString(elementHash).toString()
            pFactory.addType(output_entity, checkAux, qn(ProvenanceType.fileChecksum.toString()))

            Object sizeAux = fileAux.length()
            pFactory.addType(output_entity, sizeAux, qn(ProvenanceType.fileSize.toString()))

            /*
            Create the relation btwn ACTIVITY and the ENTITY
             */
            WasGeneratedBy generationAction = pFactory.newWasGeneratedBy(output_entity,"",activity_object)
            generationAction.setId(qn("${generatedBy_prefix}${trace.getTaskId()}_${pathString}"))
            generatedMap.put(generationAction.getId(),generationAction)

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
