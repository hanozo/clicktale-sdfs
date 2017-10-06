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
    public Response create(MakeDirCommand cmd) {
        try {
            validate(cmd.getPath());
            domain.make(cmd);
            return Response
                    .ok()
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
    public Response remove(RemoveDirCommand cmd) {
        try {
            validate(cmd.getPath());
            domain.remove(cmd);
            return Response
                    .ok()
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
    public Response rename(RenameDirCommand cmd) {
        try {
            validate(cmd.getOldName());
            validate(cmd.getNewName());
            domain.rename(cmd);
            return Response
                    .ok()
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
     * @param path
     * @throws InvalidPathException
     *          if the path string cannot be converted to a {@code Path}
     */
    private void validate(CharSequence path) throws InvalidPathException {

        Paths.get(path.toString());
    }

}
