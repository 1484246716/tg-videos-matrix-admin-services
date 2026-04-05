import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';

@Injectable()
export class InternalTokenGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<{ headers?: Record<string, string | string[]> }>();
    const tokenHeader = request.headers?.['x-internal-token'];
    const token = Array.isArray(tokenHeader) ? tokenHeader[0] : tokenHeader;

    const expected = process.env.API_INTERNAL_TOKEN?.trim();
    if (!expected) {
      throw new UnauthorizedException('API_INTERNAL_TOKEN not configured');
    }

    if (!token || token !== expected) {
      throw new UnauthorizedException('invalid internal token');
    }

    return true;
  }
}
