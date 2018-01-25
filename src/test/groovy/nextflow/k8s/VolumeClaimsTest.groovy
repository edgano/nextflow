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

package nextflow.k8s

import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class VolumeClaimsTest extends Specification {

    def 'should find volume by path' () {

        when:
        def volumes = new VolumeClaims( [ foo: [mountPath: '/data/work'], bar: [mountPath: '/other/path'] ] )
        then:
        volumes.findVolumeByPath('/data/work') == 'foo'
        volumes.findVolumeByPath('/other/path') == 'bar'
        volumes.findVolumeByPath('/data') == null

    }

    def 'should collect mount paths' () {

        when:
        def volumes = new VolumeClaims( [ foo: [mountPath: '/data/work'], bar: [mountPath: '/other/path'] ] )
        then:
        volumes.getMountPaths() == ['/data/work', '/other/path']

    }

    def 'should sanitize paths' () {
        given:
        def VOLS = [
                vol1: [mountPath: '/data/work//'],
                vol2: [mountPath: '//'],
                vol3: [mountPath: '/data']]

        when:
        new VolumeClaims(VOLS)
        then:
        VOLS.vol1.mountPath == '/data/work'
        VOLS.vol2.mountPath == '/'
        VOLS.vol3.mountPath == '/data'
    }
}
