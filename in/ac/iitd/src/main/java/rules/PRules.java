package rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

import convention.PConvention;
import rel.PProjectFilter;
import rel.PTableScan;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.plan.RelOptRule.operand;


public class PRules {

    private PRules(){
    }

    public static final RelOptRule P_TABLESCAN_RULE = new PTableScanRule(PTableScanRule.DEFAULT_CONFIG);

    private static class PTableScanRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalTableScan.class,
                        Convention.NONE, PConvention.INSTANCE,
                        "PTableScanRule")
                .withRuleFactory(PTableScanRule::new);

        protected PTableScanRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {

            TableScan scan = (TableScan) relNode;
            final RelOptTable relOptTable = scan.getTable();

            if(relOptTable.getRowType() == scan.getRowType()) {
                return PTableScan.create(scan.getCluster(), relOptTable);
            }

            return null;
        }
    }

    // Write a class PProjectFilterRule that converts a LogicalProject followed by a LogicalFilter to a single PProjectFilter node.

    // You can make any changes starting here.
    public static class PProjectFilterRule extends RelOptRule {

        public static final PProjectFilterRule INSTANCE = new PProjectFilterRule();

        public PProjectFilterRule() {
            super(operand(LogicalProject.class, operand(LogicalFilter.class, any())), "ProjectFilterRule");
        }

        @Override
        public void onMatch(RelOptRuleCall relOptRuleCall) {
//            System.out.println("On Match");
            LogicalProject project = relOptRuleCall.rel(0);
            LogicalFilter filter = relOptRuleCall.rel(1);

            HepRelVertex rel_vertex = (HepRelVertex) filter.getInput();
            RelNode input = rel_vertex.getCurrentRel();
//            System.out.println("Hi: " + project);
//            System.out.println("Hiiiii: " + filter);
//            System.out.println("Hiiiii: " + input);
            PProjectFilter prel = handleNested(project, filter);
            relOptRuleCall.transformTo(prel);
        }

        private PProjectFilter handleNested(LogicalProject project, LogicalFilter filter) {
//            System.out.println("handle nested");
            RelNode relVertex = filter.getInput();
            if (relVertex instanceof HepRelVertex) {
                RelNode input = extractInput(((HepRelVertex) relVertex).getCurrentRel());
                if (input != null) {
                    return new PProjectFilter(
                            project.getCluster(),
                            project.getTraitSet(),
                            input,
                            project.getProjects(),
                            project.getRowType(),
                            filter.getCondition()
                    );
                }
            }

            return null;
        }

        private RelNode extractInput(RelNode input) {
            if (input instanceof TableScan) {
                return (TableScan) input;
            } else if (input instanceof LogicalProject) {
                LogicalProject nestedProject = (LogicalProject) input;
                LogicalFilter nestedFilter = findNestedFilter(nestedProject);
//                System.out.println("nested project: " + nestedProject);
//                System.out.println("nested filter: " + nestedFilter);
                if (nestedFilter != null) {
                    return handleNested(nestedProject, nestedFilter);
                }
            }
            return null;
        }

        private LogicalFilter findNestedFilter(LogicalProject project) {
            RelNode input = project.getInput();
            if (input instanceof HepRelVertex) {
                RelNode currentRel = ((HepRelVertex) input).getCurrentRel();
                if (currentRel instanceof LogicalFilter) {
                    return (LogicalFilter) currentRel;
                }
            }
            return null;
        }

    }

}
