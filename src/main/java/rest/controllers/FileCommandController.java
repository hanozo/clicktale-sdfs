package rest.controllers;

import avro.commands.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rest.domain.FileDomain;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;

/**
 * File commands REST API
 */
@Path("/files")
public class FileCommandController {

    @Context
    FileDomain domain;

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
    public Response create(CreateFileCommand cmd) {
        try {
            validate(cmd.getFile());
            BareResponse bare = domain.create(cmd).get();

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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/{file}")
    public Response remove(@PathParam("file") String file) {
        try {
            validate(file);
            BareResponse bare = domain.remove(new RemoveFileCommand(file)).get();

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
    public Response update(UpdateFileCommand cmd) {
        try {
            validate(cmd.getFile());
            BareResponse bare = domain.update(cmd).get();

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
     * @param path full path file name
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
