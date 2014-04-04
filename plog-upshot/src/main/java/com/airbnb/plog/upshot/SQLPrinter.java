package com.airbnb.plog.upshot;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.RowConstructorNode;
import com.foundationdb.sql.parser.ValueNodeList;
import com.foundationdb.sql.unparser.NodeToString;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;


@NotThreadSafe
public final class SQLPrinter extends NodeToString {
    // Realistically safe size. Otherwise, it's just a runtime exception...
    private int[] cardinalities = new int[32];

    private int cardinalitiesIndex;

    /*
    Allows to reuse the SQLPrinter for another request.
     */
    public void reset() {
        cardinalitiesIndex = 0;
    }

    public int[] getCardinalities() {
        return Arrays.copyOfRange(cardinalities, 0, cardinalitiesIndex + 1);
    }

    @Override
    protected String constantNode(ConstantNode node) throws StandardException {
        return "?";
    }

    @Override
    protected String rowCtorNode(RowConstructorNode row) throws StandardException {
        final ValueNodeList nodeList = row.getNodeList();

        cardinalities[cardinalitiesIndex] = nodeList.size();
        cardinalitiesIndex++;

        return "!";
    }
}
