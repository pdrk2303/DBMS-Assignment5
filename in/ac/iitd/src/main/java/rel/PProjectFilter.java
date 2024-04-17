package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

import java.util.List;

/*
    * PProjectFilter is a relational operator that represents a Project followed by a Filter.
    * You need to write the entire code in this file.
    * To implement PProjectFilter, you can extend either Project or Filter class.
    * Define the constructor accordinly and override the methods as required.
*/
public class PProjectFilter implements PRel {

    public String toString() {
        return "PProjectFilter";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        /* Write your code here */
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProjectFilter has next");
        /* Write your code here */
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        /* Write your code here */
        return null;
    }
}
