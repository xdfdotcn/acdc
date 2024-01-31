package cn.xdf.acdc.devops.service.process.widetable.sql.transformer;

import cn.xdf.acdc.devops.service.error.exceptions.AcdcServiceException;
import cn.xdf.acdc.devops.service.process.datasystem.definition.DataFieldDefinition;
import cn.xdf.acdc.devops.service.process.widetable.sql.CalciteMysqlFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Result;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class PredicatePushDownTransformer implements SqlTransformer<Map<String, Map<String, List<DataFieldDefinition>>>> {
    private static final int OPT_LIMIT = 20;
    
    private CalciteMysqlFactory calciteMysqlFactory = new CalciteMysqlFactory();
    
    @Override
    public SqlNode transform(final SqlNode sqlNode, final Map<String, Map<String, List<DataFieldDefinition>>> schemaInfo) {
        RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        Prepare.CatalogReader catalogReader = calciteMysqlFactory.createCatalogReader(schemaInfo);
        SqlValidator validator = calciteMysqlFactory.createValidator(schemaInfo);
        SqlNode validatedSqlNode = validator.validate(sqlNode);
        
        // convert to RelNode
        RexBuilder rexBuilder = new RexBuilder(factory);
        HepPlanner planner = new HepPlanner(getOptRules());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);
        SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);
        
        // sql optimization
        for (int i = 0; i < OPT_LIMIT; i++) {
            RelNode relNode = converter.convertSelect((SqlSelect) validatedSqlNode, true);
            
            // finds the most efficient expression to implement this query
            cluster.getPlanner().setRoot(relNode);
            RelNode bestExp = cluster.getPlanner().findBestExp();
            
            // convert to SqlNode
            RelToSqlConverter relToSqlConverter = new RelToSqlConverter(MysqlSqlDialect.DEFAULT);
            Result result = relToSqlConverter.visitRoot(bestExp);
            // 再次执行校验，避免 select * 的情况出现
            SqlNode validatedStatement;
            try {
                validatedStatement = validator.validate(result.asStatement());
                // CHECKSTYLE:OFF
            } catch (Exception e) {
                // CHECKSTYLE:ON
                log.warn("After predicate push down, the sql verification error occurred", e);
                continue;
            }
            
            if (!validatedStatement.toString().equals(validatedSqlNode.toString())) {
                log.warn("The predicate push down optimization is not optimal, "
                        + "and did not reach the legal limit of {} times should continue to execute.", OPT_LIMIT);
                validatedSqlNode = validatedStatement;
                continue;
            }
            
            return validatedStatement;
        }
        
        throw new AcdcServiceException("The number of optimizations reached the maximum limit :" + OPT_LIMIT);
    }
    
    private HepProgram getOptRules() {
        HepProgramBuilder builder = new HepProgramBuilder();
        // TODO 可以重复传入多次 rule 谓词可以逐层下推，for (int i = 0; i < optLimit; i++) { }
        builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        builder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
        builder.addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE);
        
        return builder.build();
    }
    
    @Override
    public String getName() {
        return this.getClass().getName();
    }
}
