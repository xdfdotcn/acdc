package cn.xdf.acdc.devops.service.utility.datasystem.helper;

import cn.xdf.acdc.devops.service.error.exceptions.ServerErrorException;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public abstract class AbstractMysqlHelperService {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.warn("Init mysql jdbc driver exception", e);
            throw new ServerErrorException(e);
        }
    }

    protected Connection createConnection(
        final String rdbInstanceUrl,
        final String user,
        final String password) {
        try {
            return DriverManager.getConnection(
                rdbInstanceUrl,
                user,
                password);
        } catch (SQLException e) {
            throw new ServerErrorException(String.format("Can not connect to %s with user %s", rdbInstanceUrl, user), e);
        }
    }

    protected void close(final Connection conn, final Statement stmt, final ResultSet rs) {
        if (Objects.nonNull(conn)) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }
        }
        if (Objects.nonNull(stmt)) {
            try {
                stmt.close();
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }
        }

        if (Objects.nonNull(rs)) {
            try {
                rs.close();
            } catch (SQLException e) {
                throw new ServerErrorException(e);
            }
        }
    }

    protected <R> R executeQuery(
        final Connection connection,
        final String sql,
        final Function<ResultSet, R> callback) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = connection;
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery();
            R result = callback.apply(rs);
            return result;
        } catch (SQLException e) {
            throw new ServerErrorException(e);
        } finally {
            close(conn, stmt, rs);
        }
    }

    protected <R> R executeQuery(
        final Connection connection,
        final String sql,
        final Consumer<PreparedStatement> prepareFun,
        final Function<ResultSet, R> callback
    ) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = connection;
            stmt = conn.prepareStatement(sql);
            prepareFun.accept(stmt);
            rs = stmt.executeQuery();
            R result = callback.apply(rs);
            return result;
        } catch (SQLException e) {
            throw new ServerErrorException(e);
        } finally {
            close(conn, stmt, rs);
        }
    }
}
