package github.cheneykwok.sqldistinct.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author gzc
 * @date 2023-11-06
 */
@Getter
@Setter
@ToString
@AllArgsConstructor
public class SqlParseData {

    private PlainSelect select;

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlParseData that = (SqlParseData) o;
        return equalsSelect(this.select, that.select);
    }


    public boolean equalsSelect(PlainSelect thisSelect, PlainSelect thatSelect) {
        if (thisSelect == thatSelect) {
            return true;
        }
        if (thisSelect == null || thatSelect == null) {
            throw new RuntimeException();
        }
        if (thisSelect.getClass() != thatSelect.getClass()) {
            return false;
        }

        FromItem thisFromItem = thisSelect.getFromItem();
        FromItem thatFromItem = thatSelect.getFromItem();
        if (thisFromItem != null && thatFromItem != null) {
            // 比较 FromItem
            if (!equalsFromItem(thisFromItem, thatFromItem)) {
                return false;
            }
        } else if (thisFromItem != null) {
            return false;
        } else if (thatFromItem == null) {
            return false;
        }


        // 比较 Join
        List<Join> thisJoins = thisSelect.getJoins();
        List<Join> thatJoins = thatSelect.getJoins();
        if (thisJoins != null && thatJoins != null) {
            if (thisJoins.size() != thatJoins.size()) {
                return false;
            }
            for (int i = 0; i < thisJoins.size(); i++) {
                Join thisJoin = thisJoins.get(i);
                Join thatJoin = thatJoins.get(i);
                if (!equalsFromItem(thisJoin.getFromItem(), thatJoin.getFromItem())) {
                    return false;
                }
                // 比较 ON
                LinkedList<Expression> thisJoinOnExpressions = (LinkedList<Expression>) thisJoin.getOnExpressions();
                LinkedList<Expression> thatJoinOnExpressions = (LinkedList<Expression>) thatJoin.getOnExpressions();
                if (thisJoinOnExpressions.size() != thatJoinOnExpressions.size()) {
                    return false;
                }
                for (int j = 0; j < thisJoinOnExpressions.size(); j++) {
                    Expression thisJoinOnExpression = getRealExpression(thisJoinOnExpressions.get(j));
                    Expression thatJoinOnExpression = getRealExpression(thatJoinOnExpressions.get(j));
                    if (thisJoinOnExpression.getClass() != thatJoinOnExpression.getClass()) {
                        return false;
                    }
                    if (thisJoinOnExpression instanceof BinaryExpression) {
                        BinaryExpression thisBinaryExpression = (BinaryExpression) thisJoinOnExpression;
                        BinaryExpression thatBinaryExpression = (BinaryExpression) thatJoinOnExpression;
                        if (!equalsBinaryExpression(thisBinaryExpression, thatBinaryExpression)) {
                            return false;
                        }
                    }
                }
            }
        } else if (thisJoins != null) {
            return false;
        } else if (thatJoins != null) {
            return false;
        }

        // 比较 where
        Expression thisWhere = thisSelect.getWhere();
        Expression thatWhere = thatSelect.getWhere();
        if (thisWhere != null && thatWhere != null) {
            if (thisWhere.getClass() != thatWhere.getClass()) {
                return false;
            }
            if (thisWhere instanceof BinaryExpression) {
                BinaryExpression thisBinaryExpression = (BinaryExpression) thisWhere;
                BinaryExpression thatBinaryExpression = (BinaryExpression) thatWhere;
                if (!equalsBinaryExpression(thisBinaryExpression, thatBinaryExpression)) {
                    return false;
                }
            }
        } else if (thisWhere != null) {
            return false;
        } else if (thatWhere != null) {
            return false;
        }

        // 比较 groupBy
        GroupByElement thisGroupBy = thisSelect.getGroupBy();
        GroupByElement thatGroupBy = thatSelect.getGroupBy();
        if (thisGroupBy != null && thatGroupBy != null) {
            ExpressionList<Expression> thisGroupByExpressions = thisGroupBy.getGroupByExpressionList();
            ExpressionList<Expression> thatGroupByExpressions = thatGroupBy.getGroupByExpressionList();
            if (thisGroupByExpressions != null && thatGroupByExpressions != null) {
                if (thisGroupByExpressions.size() != thatGroupByExpressions.size()) {
                    return false;
                }
                for (int i = 0; i < thisGroupByExpressions.size(); i++) {
                    Expression thisExpression = thisGroupByExpressions.get(i);
                    Expression thatExpression = thatGroupByExpressions.get(i);
                    if (thisExpression.getClass() != thatExpression.getClass()) {
                        return false;
                    }
                    if (thisExpression instanceof Column) {
                        if (!equalsColumn(thisExpression, thatExpression)) {
                            return false;
                        }
                    }
                    if (thisExpression instanceof BinaryExpression) {
                        BinaryExpression thisBinaryExpression = (BinaryExpression) thisExpression;
                        BinaryExpression thatBinaryExpression = (BinaryExpression) thatExpression;
                        if (!equalsBinaryExpression(thisBinaryExpression, thatBinaryExpression)) {
                            return false;
                        }
                    }
                }
            } else if (thisGroupByExpressions != null) {
                return false;
            } else if (thatGroupByExpressions != null) {
                return false;
            }

        } else if (thisGroupBy != null) {
            return false;
        } else if (thatGroupBy != null) {
            return false;
        }
        return true;
    }

    private boolean equalsFromItem(FromItem thisFromItem, FromItem thatFromItem) {

        if (thisFromItem.getClass() != thatFromItem.getClass()) {
            return false;
        }
        if (thisFromItem instanceof Table) {
            Table thisTable = (Table) thisFromItem;
            Table thatTable = (Table) thatFromItem;
            if (thisTable.getNameParts().size() != thatTable.getNameParts().size()) {
                return false;
            }
            // 比较表名
            if (!Objects.equals(thisTable.getNameParts().get(0), thatTable.getNameParts().get(0))) {
                return false;
            }
        }
        if (thisFromItem instanceof ParenthesedSelect) {
            ParenthesedSelect thisParenthesedSelect = (ParenthesedSelect) thisFromItem;
            ParenthesedSelect thatParenthesedSelect = (ParenthesedSelect) thatFromItem;
            return equalsSelect((PlainSelect) thisParenthesedSelect.getSelect(), (PlainSelect) thatParenthesedSelect.getSelect());
        }
        return true;
    }

    public boolean equalsBinaryExpression(BinaryExpression thisBinaryExpression, BinaryExpression thatBinaryExpression) {
        if (thisBinaryExpression.getClass() != thatBinaryExpression.getClass()) {
            return false;
        }
        Expression thisLeftExpression = thisBinaryExpression.getLeftExpression();
        Expression thatLeftExpression = thatBinaryExpression.getLeftExpression();
        if (!equalExpression(thisLeftExpression, thatLeftExpression)) {
            return false;
        }
        Expression thisRightExpression = thisBinaryExpression.getRightExpression();
        Expression thatRightExpression = thatBinaryExpression.getRightExpression();
        return equalExpression(thisRightExpression, thatRightExpression);
    }


    public boolean equalExpression(Expression thisLeftExpression, Expression thatLeftExpression) {
        thisLeftExpression = getRealExpression(thisLeftExpression);
        thatLeftExpression = getRealExpression(thatLeftExpression);
        if (thisLeftExpression.getClass() != thatLeftExpression.getClass()) {
            return false;
        }
        if (thisLeftExpression instanceof Column) {
            return equalsColumn(thisLeftExpression, thatLeftExpression);
        }
        if (thisLeftExpression instanceof LikeExpression) {
            LikeExpression thisLikeExpression = (LikeExpression) thisLeftExpression;
            LikeExpression thatLikeExpression = (LikeExpression) thatLeftExpression;
            return equalsColumn(thisLikeExpression.getLeftExpression(), thatLikeExpression.getLeftExpression());
        }
//        if (thisLeftExpression instanceof GreaterThan || thisLeftExpression instanceof GreaterThanEquals ||
//                thisLeftExpression instanceof MinorThan || thisLeftExpression instanceof MinorThanEquals ||
//                thisLeftExpression instanceof NotEqualsTo || thisLeftExpression instanceof EqualsTo) {
        if (thisLeftExpression instanceof ComparisonOperator) {
            ComparisonOperator thisComparisonOperator = (ComparisonOperator) thisLeftExpression;
            ComparisonOperator thatComparisonOperator = (ComparisonOperator) thatLeftExpression;
            return equalsColumn(thisComparisonOperator.getLeftExpression(), thatComparisonOperator.getLeftExpression());
        }
        if (thisLeftExpression instanceof BinaryExpression) {
            return equalsBinaryExpression((BinaryExpression) thisLeftExpression, (BinaryExpression) thatLeftExpression);
        }
        return true;

    }

    public Expression getRealExpression(Expression expression) {
        if (expression instanceof Parenthesis) {
            return ((Parenthesis) expression).getExpression();
        }
        return expression;
    }

    public boolean equalsColumn(Expression e1, Expression e2) {
        if (e1.getClass() != e2.getClass()) {
            return false;
        }
        if (e1 instanceof Column) {
            Column c1 = (Column) e1;
            Column c2 = (Column) e2;
            if (!c1.getColumnName().equals(c2.getColumnName())) {
                return false;
            }
            if (c1.getTable() != null && c2.getTable() != null) {
                return c1.getTable().getName().equals(c2.getTable().getName());
            }else if (c1.getTable() != null) {
                return false;
            }else if (c2.getTable() != null) {
                return false;
            }

        }
        return true;
    }
}
