/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.trace
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

import groovy.json.JsonOutput
import groovy.text.GStringTemplateEngine
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskId
import nextflow.processor.TaskProcessor
import nextflow.script.WorkflowMetadata
/**
 * Render pipeline report processes execution.
 * Based on original TimelineObserver code by Paolo Di Tommaso
 *
 * @author Phil Ewels <phil.ewels@scilifelab.se>
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class ReportObserver implements TraceObserver {

    static final String DEF_FILE_NAME = 'report.html'

    static final int DEF_MAX_TASKS = 10_000

    /**
     * Holds the the start time for tasks started/submitted but not yet completed
     */
    final private Map<TaskId,TraceRecord> records = new LinkedHashMap<>()

    /**
     * Holds a report summary instance for each process group
     */
    final private Map<String,ReportSummary> summaries = new HashMap<>()

    /**
     * Holds workflow session
     */
    private Session session

    /**
     * The path the HTML report file created
     */
    private Path reportFile

    /**
     * Max number of tasks allowed in the report, when they exceed this
     * number the tasks table is omitted
     */
    private int maxTasks = DEF_MAX_TASKS

    /**
     * Executor service used to compute report summary stats
     */
    private ExecutorService executor

    /**
     * Creates a report observer
     *
     * @param file The file path where to store the resulting HTML report document
     */
    ReportObserver( Path file ) {
        this.reportFile = file
    }

    /**
     * Enables the collection of the task executions metrics in order to be reported in the HTML report
     *
     * @return {@code true}
     */
    @Override
    boolean enableMetrics() {
        return true
    }

    /**
     * @return The {@link WorkflowMetadata} object associated to this execution
     */
    protected WorkflowMetadata getWorkflowMetadata() {
        session.binding.getVariable('workflow') as WorkflowMetadata
    }

    /**
     * @return The map of collected {@link TraceRecord}s
     */
    protected Map<TaskId,TraceRecord> getRecords() {
        records
    }

    /**
     * Set the number max allowed tasks. If this number is exceed the the tasks
     * json in not included in the final report
     *
     * @param value The number of max task record allowed to be included in the HTML report
     * @return The {@link ReportObserver} itself
     */
    ReportObserver setMaxTasks( int value ) {
        this.maxTasks = value
        return this
    }

    /**
     * Create the trace file, in file already existing with the same name it is
     * "rolled" to a new file
     */
    @Override
    void onFlowStart(Session session) {
        this.session = session
        this.executor = session.getExecService()
    }

    /**
     * Save the pending processes and close the trace file
     */
    @Override
    void onFlowComplete() {
        log.debug "Flow completing -- rendering html report"
        renderHtml()
    }

    /**
     * This method is invoked when a process is created
     *
     * @param process A {@link TaskProcessor} object representing the process created
     */
    @Override
    void onProcessCreate(TaskProcessor process) { }


    /**
     * This method is invoked before a process run is going to be submitted
     *
     * @param handler A {@link TaskHandler} object representing the task submitted
     */
    @Override
    void onProcessSubmit(TaskHandler handler) {
        log.trace "Trace report - submit process > $handler"
        final trace = handler.getTraceRecord()
        synchronized (records) {
            records[ trace.taskId ] = trace
        }
    }

    /**
     * This method is invoked when a process run is going to start
     *
     * @param handler  A {@link TaskHandler} object representing the task started
     */
    @Override
    void onProcessStart(TaskHandler handler) {
        log.trace "Trace report - start process > $handler"
        def trace = handler.getTraceRecord()
        synchronized (records) {
            records[ trace.taskId ] = trace
        }
    }

    /**
     * This method is invoked when a process run completes
     *
     * @param handler A {@link TaskHandler} object representing the task completed
     */
    @Override
    void onProcessComplete(TaskHandler handler) {
        log.trace "Trace report - complete process > $handler"
        final record = handler.getTraceRecord()
        if( !record ) {
            log.debug "WARN: Unable to find trace record for task id=${handler.task?.id}"
            return
        }

        synchronized (records) {
            records[ record.taskId ] = record
            aggregate(record)
        }
    }

    /**
     * This method is invoked when a process run cache is hit
     *
     * @param handler A {@link TaskHandler} object representing the task cached
     */
    @Override
    void onProcessCached(TaskHandler handler) {
        log.trace "Trace report - cached process > $handler"
        final record = handler.getTraceRecord()

        // remove the record from the current records
        synchronized (records) {
            records[ record.taskId ] = record
            aggregate(record)
        }
    }

    /**
     * Aggregates task record for each process in order to render the
     * final execution stats
     *
     * @param record A {@link TraceRecord} object representing a task executed
     */
    protected void aggregate(TraceRecord record) {
        def process = record.get('process') as String
        def summary = summaries.get(process)
        if( !summary ) {
            summaries.put(process, summary=new ReportSummary())
        }
        summary.add(record)
    }

    /**
     * @return The tasks json payload
     */
    protected String renderTasksJson() {
        final r = getRecords()
        r.size()<=maxTasks ? renderJsonData(r.values()) : '[]'
    }

    /**
     * @return The execution summary json
     */
    protected String renderSummaryJson() {
        final summary = computeSummary()
        final result = JsonOutput.toJson(summary)
        log.debug "Execution report summary data:\n${JsonOutput.prettyPrint(result).indent()}"
        return result
    }

    /**
     * Compute the workflow process summary stats
     *
     * @return A {@link Map} holding the summary stats for each process
     */
    protected Map computeSummary() {

        // summary stats can be expensive on big workflow
        // speed-up the computation using a parallel
        List<Callable<List>> tasks = []
        summaries.keySet().each  { String process ->
            final summary = summaries[process]
            summary.names.each { String series ->
                // the task execution turn a triple
                tasks << { return [ process, series, summary.compute(series)] } as Callable<List>
            }
        }

        // submit the parallel execution
        final allResults = executor.invokeAll(tasks)

        // compose the final result
        def result = new HashMap(summaries.size())
        allResults.each { Future<List> future ->
            final triple = future.get()
            final name = triple[0]      // the process name
            final series = triple[1]    // the series name eg. `cpu`, `time`, etc
            final summary = triple[2]   // the computed summary
            final process = (Map)result.getOrCreate(name, [:])
            process[series] = summary
        }

        return result
    }

    protected String renderPayloadJson() {
        "{ \"trace\":${renderTasksJson()}, \"summary\":${renderSummaryJson()} }"
    }

    /**
     * Render the report HTML document
     */
    protected void renderHtml() {

        // render HTML report template
        final tpl_fields = [
            workflow : getWorkflowMetadata(),
            payload : renderPayloadJson(),
            assets_css : [
                readTemplate('assets/bootstrap.min.css'),
                readTemplate('assets/datatables.min.css')
            ],
            assets_js : [
                readTemplate('assets/jquery-3.2.1.min.js'),
                readTemplate('assets/popper.min.js'),
                readTemplate('assets/bootstrap.min.js'),
                readTemplate('assets/datatables.min.js'),
                readTemplate('assets/moment.min.js'),
                readTemplate('assets/plotly.min.js'),
                readTemplate('assets/ReportTemplate.js')
            ]
        ]
        final tpl = readTemplate('ReportTemplate.html')
        def engine = new GStringTemplateEngine()
        def html_template = engine.createTemplate(tpl)
        def html_output = html_template.make(tpl_fields).toString()

        // make sure the parent path exists
        def parent = reportFile.getParent()
        if( parent )
            Files.createDirectories(parent)

        // roll the any trace files that may exist
        reportFile.rollFile()

        def writer = Files.newBufferedWriter(reportFile, Charset.defaultCharset())
        writer.withWriter { w -> w << html_output }
        writer.close()
    }

    /**
     * Render the executed tasks json payload
     *
     * @param data A collection of {@link TraceRecord}s representing the tasks executed
     * @return The rendered json payload
     */
    protected String renderJsonData(Collection<TraceRecord> data) {
        def List<String> formats = null
        def List<String> fields = null
        def result = new StringBuilder()
        result << '[\n'
        int i=0
        for( TraceRecord record : data ) {
            if( i++ ) result << ','
            if( !formats ) formats = TraceRecord.FIELDS.values().collect { it!='str' ? 'num' : 'str' }
            if( !fields ) fields = TraceRecord.FIELDS.keySet() as List
            record.renderJson(result,fields,formats)
        }
        result << ']'
        return result.toString()
    }

    /**
     * Read the document HTML template from the application classpath
     *
     * @param path A resource path location
     * @return The loaded template as a string
     */
    private String readTemplate( String path ) {
        StringWriter writer = new StringWriter();
        def res =  this.class.getResourceAsStream( path )
        int ch
        while( (ch=res.read()) != -1 ) {
            writer.append(ch as char);
        }
        writer.toString();
    }


}
