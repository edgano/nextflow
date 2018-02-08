package nextflow.cli

import nextflow.exception.AbortOperationException
import nextflow.CommandLine


trait Prompt {

    @CommandLine.Option(names=['-y'], description = 'Answer yes for all questions')
    boolean yes

    /**
     * Prompt the user for confirmation
     *
     * @param message The message to show when asking the user confirmation
     * @throws nextflow.exception.AbortOperationException when the user answer NO
     */
    void prompt(String message) {
        if( yes ) return
        print "$message [y/n] "
        char answer = System.in.read()
        if( answer != 'y' && answer != 'Y' ) throw new AbortOperationException()
    }
}