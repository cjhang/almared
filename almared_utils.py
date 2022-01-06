import os
import re
import glob

def split_sources(msfile, match='HATLAS_RED_\d+', datacolumn='corrected', 
        outdir='./', spw='5,7,17,19', overwrite=False, **kwargs):
    """this function is used to extract every single target in 
    the snapshoot observation
    """
    if not os.path.isdir(msfile):
        raise ValueError("Cannot find {}".format(msfile))
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))

    field_match = re.compile(match)
    tb.open(os.path.join(msfile, 'FIELD'))
    field_names = tb.getcol('NAME')
    for name in field_names:
        if field_match.match(name):
            ms_field = os.path.join(outdir, name + '.ms')
            if overwrite:
                if os.path.isdir(ms_field):
                    rmtables(ms_field)
            else:
                if os.path.isdir(ms_field):
                    ms_field = ms_field + '.duplicate'
            print('Splitting {}...'.format(name))
            split(vis=msfile, outputvis=ms_field, datacolumn=datacolumn,
                    field=name, spw=spw, **kwargs)


def make_cont_image(vis, basename=None, outdir='./',
        datacolumn='data', imsize=200, cellsize='0.2arcsec', 
        outframe='LSRK', specmode='mfs', 
        threshold='0.2mJy', niter=1000, interactive=False,        
        weighting='natural', usemask='auto-multithresh', 
        clean=True, only_fits=True, debug=False, pbcor=True, overwrite=False,
        **kwargs):
    """the quick function to generate the continuum image
    """
    if basename is None:
        basename = os.path.basename(vis)[:-3]
    if not os.path.isdir(outdir):
        os.system('mkdir {}'.format(outdir))

    imagename = os.path.join(outdir, basename + '.mfs')

    if debug:
        # print out all the parameters
        print("imagename: {}".format(imagename))
        print("imsize: {}".format(imsize))
        print("cellsize: {}".format(cellsize))
        print("threshold: {}".format(threshold))
        print("weighting: {}".format(weighting))
        print("usemask: {}".format(usemask))

    new_clean = True
    if clean:
        if os.path.isfile(imagename+'.image.fits') or os.path.isdir(imagename+'.image'):
            if not overwrite:
                print("{} image already exists. Delete them to overwrite.".format('imagename'))
                new_clean = False
    if new_clean:
        rmtables('{}.*'.format(imagename))
        os.system('rm -rf {}.fits'.format(imagename))
        tclean(vis=vis,
                imagename=imagename,
                imsize=imsize, 
                cell=cellsize, 
                datacolumn=datacolumn, 
                specmode=specmode, 
                outframe=outframe, 
                weighting=weighting,
                niter=niter, 
                threshold=threshold,
                interactive=interactive, 
                usemask=usemask,
                **kwargs)
        if pbcor:
            impbcor(imagename=imagename+'.image',
                    pbimage=imagename+'.pb',
                    outfile=imagename+'.pbcor.image')
        if only_fits:
            exportfits(imagename=imagename+'.image', fitsimage=imagename+'.image.fits')
            exportfits(imagename=imagename+'.pbcor.image', fitsimage=imagename+'.pbcor.image.fits')
            exportfits(imagename=imagename+'.psf', fitsimage=imagename+'.psf.fits')
            exportfits(imagename=imagename+'.pb', fitsimage=imagename+'.pb.fits')
            rmtables(tablenames=imagename+'.*')
    else:
        return 0

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
