package nextflow.trace

import org.openprovenance.prov.interop.InteropFramework
import org.openprovenance.prov.model.Entity
import org.openprovenance.prov.model.Namespace
import org.openprovenance.prov.model.ProvFactory
import org.openprovenance.prov.model.QualifiedName
import spock.lang.Specification

/**
 * Created by edgar on 31/05/18.
 */
class EntityTest extends Specification{
    public static final String PROVBOOK_NS = "http://www.Workflow_REPO.org";
    public static final String PROVBOOK_PREFIX = "NF_prov";

    private final ProvFactory pFactory= InteropFramework.newXMLProvFactory();

    def testEntity() {
        when:
        final Namespace ns=new Namespace();
        ns.addKnownNamespaces();
        ns.register(PROVBOOK_PREFIX, PROVBOOK_NS);

        def files = ['a','b','c']
        LinkedList<Entity> entityList = new LinkedList<Entity>()

        for( String name : files ) {
            QualifiedName qn = ns.qualifiedName(PROVBOOK_PREFIX, name, pFactory);
            Entity input_entity = pFactory.newEntity(qn);
            entityList.add(input_entity)
        }
        then:
        entityList.size()==3

    }
}
