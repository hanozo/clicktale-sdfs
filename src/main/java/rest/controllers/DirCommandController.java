package rest.controllers;

import avro.commands.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rest.domain.DirDomain;

import javax.ws.rs.*;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.file.*;

/**
 * Directory commands REST API
 */
@Path("/dir")
public class DirCommandController {

    @Context
    DirDomain domain;

    private static final Logger logger = LogManager.getLogger();

    @GET
    public Response ruok() {
        return Response
                .ok()
                .entity("imok")
                .build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response create(MakeDirCommand cmd) {
        try {
            validate(cmd.getPath());
            BareResponse bare = domain.make(cmd).get();

            Result res = new Result();
            res.setSucceeded(bare.getSucceeded());
            res.setFeedback(bare.getFeedback());

            return Response
                    .ok()
                    .entity(res)
                    .build();
        } catch (Exception e) {
            logger.error(e);
            return Response
                    .serverError()
                    .build();
        }
    }


    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response remove(RemoveDirCommand cmd) {
        try {
            validate(cmd.getPath());
            BareResponse bare = domain.remove(cmd).get();

            Result res = new Result();
            res.setSucceeded(bare.getSucceeded());
            res.setFeedback(bare.getFeedback());

            return Response
                    .ok()
                    .entity(res)
                    .build();
        } catch (Exception e) {
            logger.error(e);
            return Response
                    .serverError()
                    .build();
        }
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response rename(RenameDirCommand cmd) {
        try {
            validate(cmd.getOldName());
            validate(cmd.getNewName());
            BareResponse bare = domain.rename(cmd).get();

            Result res = new Result();
            res.setSucceeded(bare.getSucceeded());
            res.setFeedback(bare.getFeedback());

            return Response
                    .ok()
                    .entity(res)
                    .build();
        } catch (Exception e) {
            logger.error(e);
            return Response
                    .serverError()
                    .build();
        }
    }

    /**
     * Converts a path string to a {@code Path}
     *
     * @param path dir name
     * @throws InvalidPathException if the path string cannot be converted to a {@code Path}
     */
    private void validate(String path) throws InvalidPathException {

        Paths.get(path);
    }

    private static class Result {

        private boolean succeeded;
        private String feedback;

        public boolean isSucceeded() {
            return succeeded;
        }

        void setSucceeded(boolean succeeded) {
            this.succeeded = succeeded;
        }

        public String getFeedback() {
            return feedback;
        }

        void setFeedback(String feedback) {
            this.feedback = feedback;
        }
    }

}
